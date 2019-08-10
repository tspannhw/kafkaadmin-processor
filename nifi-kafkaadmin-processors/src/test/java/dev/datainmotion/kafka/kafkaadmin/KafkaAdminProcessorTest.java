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
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test the Kafka Admin Processor
 */
public class KafkaAdminProcessorTest {

    public static final String KAFKA_BROKER_URL = "demo.hortonworks.com:6667";
    public static final String FAKEURL = "fakeurl";
    private TestRunner testRunner;

    /**
     *
     */
    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(KafkaAdminProcessor.class);
    }

    /**
     *
     * @param name
     * @return
     * @throws URISyntaxException
     */
    private String pathOfResource(String name) throws URISyntaxException {
        URL r = this.getClass().getClassLoader().getResource(name);
        URI uri = r.toURI();
        return Paths.get(uri).toAbsolutePath().getParent().toString();
    }

    /**
     *
     */
    private void runAndAssertHappy() {
        try {
            testRunner.setValidateExpressionUsage(false);
            testRunner.run();
            testRunner.assertValid();

            testRunner.assertAllFlowFilesTransferred(KafkaAdminProcessor.REL_SUCCESS);
            List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(KafkaAdminProcessor.REL_SUCCESS);

            for (MockFlowFile mockFile : successFiles) {
                System.out.println("Size:" +             mockFile.getSize() ) ;
                Map<String, String> attributes =  mockFile.getAttributes();

                for (String attribute : attributes.keySet()) {
                    System.out.println("Attribute:" + attribute + " = " + mockFile.getAttribute(attribute));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * create a topic
     * @throws Exception
     */
    @Test
    public void testProcessor() throws Exception {

        java.io.File resourcesDirectory = new java.io.File("src/test/resources");
        System.out.println(resourcesDirectory.getAbsolutePath());

        // resourcesDirectory.getAbsolutePath()
        testRunner.setProperty(KafkaAdminProcessor.KAFKA_URL, KAFKA_BROKER_URL);
        testRunner.setProperty(KafkaAdminProcessor.KAFKA_TOPIC, "junit-" +
                RandomStringUtils.randomAlphabetic(25));
        testRunner.enqueue(this.getClass().getClassLoader().getResourceAsStream("flow.txt"));

        runAndAssertHappy();
    }


    /**
     *
     * @throws Exception
     */
    @Test
    public void testProcessorWithBadURL() throws Exception {

        java.io.File resourcesDirectory = new java.io.File("src/test/resources");
        System.out.println(resourcesDirectory.getAbsolutePath());

        // resourcesDirectory.getAbsolutePath()
        testRunner.setProperty(KafkaAdminProcessor.KAFKA_URL, FAKEURL);
        testRunner.setProperty(KafkaAdminProcessor.KAFKA_TOPIC, "junit-" +
                RandomStringUtils.randomAlphabetic(25));
        testRunner.enqueue(this.getClass().getClassLoader().getResourceAsStream("flow.txt"));

        runAndAssertHappy();
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testProcessorWithBadTopicName() throws Exception {

        java.io.File resourcesDirectory = new java.io.File("src/test/resources");
        System.out.println(resourcesDirectory.getAbsolutePath());

        // resourcesDirectory.getAbsolutePath()
        testRunner.setProperty(KafkaAdminProcessor.KAFKA_URL,FAKEURL);
        testRunner.setProperty(KafkaAdminProcessor.KAFKA_TOPIC,  "#&$^&#$&#*&F&D*F&*DF_- +34434 TEST");
        testRunner.enqueue(this.getClass().getClassLoader().getResourceAsStream("flow.txt"));

        runAndAssertHappy();
    }

}
