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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
//import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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

            String kafkaTopic = flowFile.getAttribute(KAFKA_URL_NAME);
            if (kafkaTopic == null) {
                kafkaTopic = context.getProperty(KAFKA_URL_NAME).evaluateAttributeExpressions(flowFile).getValue();
            }
            if (kafkaTopic == null) {
                kafkaTopic = "TopicForTim";
            }
            final String kafkaTopicFinal = kafkaTopic;

            final HashMap<String, String> attributes = new HashMap<String, String>();
            attributes.put("kafka_topic", kafkaTopicFinal);
            attributes.put("kafka_url", kafkaStringURL);

            // Contents of Flow file as a string
//            final AtomicReference<String> contentsRef = new AtomicReference<>(null);
//
//            session.read(flowFile, new InputStreamCallback() {
//                @Override
//                public void process(final InputStream input) throws IOException {
//                    final String contents = IOUtils.toString(input, "UTF-8");
//                    contentsRef.set(contents);
//                }
//            });
//
//            String contentsOfFlowFile = null;
//
//            // use this as our text
//            if (contentsRef.get() != null) {
//                contentsOfFlowFile = contentsRef.get();
//            }

            String kafkaTopicsList = kafkaAdminService.createKafkaTopic(kafkaStringURL, kafkaTopicFinal);

            if (kafkaTopicsList == null) {
                kafkaTopicsList = kafkaTopicFinal;
            }
            //List<Result> results
            if (kafkaTopicsList != null) {
                attributes.put("kafka_topic_list", kafkaTopicsList);
                // getLogger().debug(String.format("Found %d results", new Object[] { results.size() }));

//                        for (Result result : results) {
//                            attributes.put(String.format("label_%d", i), result.getLabel() );
//                            attributes.put(String.format("probability_%d",i), String.format("%.2f", result.getProbability()));
//                            attributes.put(String.format("xmin_%d", i), String.format("%.2f", result.getXmin()));
//                            i++;
//                        }

                System.err.println("Attributes size:" + attributes.size());
                if (attributes.size() == 0) {
                    session.transfer(flowFile, REL_FAILURE);
                } else {
                    flowFile = session.putAllAttributes(flowFile, attributes);

                    session.transfer(flowFile, REL_SUCCESS);
                }
            }

            session.commit();
        } catch (final Throwable t) {
            getLogger().error("Unable to create Kafka Topic " + t.getLocalizedMessage());
            throw new ProcessException(t);
        }
    }
}