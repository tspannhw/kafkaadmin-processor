package dev.datainmotion.kafka.kafkaadmin;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class KafkaAdminService {
    public KafkaAdminService() {
        super();
    }

    /**
     * create a topic in kafka
     *
     * @param kafkaURL
     * @param topicName
     * @param properties
     * @return Result
     */
    private Result createTopic(String kafkaURL, String topicName, Properties properties) {

        AdminClient adminClient = AdminClient.create(properties);

        CreateTopicsResult topicResults = null;
        Result result = new Result();
        boolean createdTopic = true;

        try {
            List<NewTopic> topicsToAdd = new ArrayList<>();
            NewTopic newTopic = null;
            newTopic = new NewTopic(topicName, 1, (short) 1);
            topicsToAdd.add(newTopic);
            topicResults = adminClient.createTopics(topicsToAdd);
            //topicResults.all().get(30, TimeUnit.SECONDS);
            Void test = topicResults.values().get(topicName).get();

            adminClient.close();
            adminClient = null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
            result.setKafkaErrorMessage(e.getLocalizedMessage());
            createdTopic = false;
        } catch (TimeoutException e) {
            e.printStackTrace();
            result.setKafkaErrorMessage(e.getLocalizedMessage());
            createdTopic = false;
        } catch (Exception e) {
            e.printStackTrace();
            result.setKafkaErrorMessage(e.getLocalizedMessage());
            createdTopic = false;
        }
        if (createdTopic) {
            result.setKafkaTopicMessage("Created topic.");
        }
        else {
            result.setKafkaTopicMessage("Could not create topic.");
        }
        return result;
    }

    /**
     * @param kafkaStringURL
     * @param kafkaTopic
     * @return String
     */
    public Result createKafkaTopic(String kafkaStringURL, String kafkaTopic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaStringURL);
        properties.put("client.id", "nifi-" + RandomStringUtils.randomAlphabetic(25));

        Result topicOutput = createTopic(kafkaStringURL, kafkaTopic, properties);
        topicOutput.setBootstrap(kafkaStringURL);
        topicOutput.setClientId(properties.get("client.id").toString());

        return topicOutput;
    }
}