/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.datainmotion.kafka.kafkaadmin;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class KafkaAdminProcessor extends AbstractProcessor {

    private KafkaAdminService kafkaAdminService = null;

    private static String KAFKA_URL_NAME = "KAFKA_URL";
    private static String KAFKA_TOPIC_NAME = "KAFKA_TOPIC";

    public static final PropertyDescriptor KAFKA_URL = new PropertyDescriptor
            .Builder().name(KAFKA_URL_NAME)
            .displayName("Kafka URL")
            .description("Kafka URL")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KAFKA_TOPIC = new PropertyDescriptor
            .Builder().name(KAFKA_TOPIC_NAME)
            .displayName("Kafka Topic to Create")
            .description("Kafka Topic to Create")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully determined image.").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to determine image.").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(KAFKA_URL);
        descriptors.add(KAFKA_TOPIC);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        System.err.println("Scheduled");
        kafkaAdminService = new KafkaAdminService();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        System.err.println("onTrigger");

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }
        try {
            flowFile.getAttributes();

            // Kafka Admin
            if (kafkaAdminService == null) {
                kafkaAdminService = new KafkaAdminService();
            }
            String kafkaURL = flowFile.getAttribute(KAFKA_URL_NAME);
            if (kafkaURL == null) {
                kafkaURL = context.getProperty(KAFKA_URL_NAME).evaluateAttributeExpressions(flowFile).getValue();
            }
            if (kafkaURL == null) {
                kafkaURL = "localhost:2181";
            }
            final String kafkaStringURL = kafkaURL;

            String kafkaTopic = flowFile.getAttribute(KAFKA_TOPIC_NAME);
            if (kafkaTopic == null) {
                kafkaTopic = context.getProperty(KAFKA_TOPIC_NAME).evaluateAttributeExpressions(flowFile).getValue();
            }
            if (kafkaTopic == null) {
                kafkaTopic = "nifi" + RandomStringUtils.randomAlphabetic(10);
            }
            final String kafkaTopicFinal = kafkaTopic;

            final HashMap<String, String> attributes = new HashMap<String, String>();
            attributes.put("kafka_topic", kafkaTopicFinal);
            attributes.put("kafka_url", kafkaStringURL);

            Result result = kafkaAdminService.createKafkaTopic(kafkaStringURL, kafkaTopicFinal);

            if (result != null) {
                attributes.put("kafka.bootstrap", result.getBootstrap());
                attributes.put("kafka.client.id", result.getClientId());
                attributes.put("kafka.error.message", result.getKafkaErrorMessage());
                attributes.put("kafka.topic.message", result.getKafkaTopicMessage());
                flowFile = session.putAllAttributes(flowFile, attributes);

                session.transfer(flowFile, REL_SUCCESS);
            } else {
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_FAILURE);
            }

            session.commit();
        } catch (final Throwable t) {
            getLogger().error("Unable to create Kafka Topic " + t.getLocalizedMessage());
            throw new ProcessException(t);
        }
    }
}