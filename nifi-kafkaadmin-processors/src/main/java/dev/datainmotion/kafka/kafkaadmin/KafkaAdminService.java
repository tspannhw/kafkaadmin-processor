package dev.datainmotion.kafka.kafkaadmin;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.*;
import java.util.concurrent.ExecutionException;
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
     * @param clientID
     * @return Result
     */
    private Result createTopic(String kafkaURL, String topicName, String clientID) {
        AdminClient adminClient = createAdminClient(kafkaURL, clientID);
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
        String clientID = "nifi-" + RandomStringUtils.randomAlphabetic(25);
        Result topicOutput = createTopic(kafkaStringURL, kafkaTopic, clientID);
        topicOutput.setBootstrap(kafkaStringURL);
        topicOutput.setClientId(clientID);
        return topicOutput;
    }


    /**
     * create admin client
     * @param kafkaStringURL
     * @return
     */
    private AdminClient createAdminClient(String kafkaStringURL, String clientID) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaStringURL);
        properties.put("client.id", clientID);
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    /**
     * list kafka topics
     * @param kafkaStringURL
     * @return list of kafka topics
     */
    public List<Result> listKafkaTopics(String kafkaStringURL) {
        String clientID = "nifi-" + RandomStringUtils.randomAlphabetic(25);
        AdminClient adminClient = createAdminClient(kafkaStringURL,clientID);
        ListTopicsResult topics = adminClient.listTopics();

        Set<String> topicNames = null;
        String errorMessage = null;
        try {
            topicNames = topics.names().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
            errorMessage = e.getLocalizedMessage();
        } catch (ExecutionException e) {
            e.printStackTrace();
            errorMessage = e.getLocalizedMessage();
        } catch (Exception e) {
            e.printStackTrace();
            errorMessage = e.getLocalizedMessage();
        }

        try {
            adminClient.close();
            adminClient = null;
        } catch (Exception e) {
            e.printStackTrace();
            errorMessage = e.getLocalizedMessage();
        }

        List<Result> results = new ArrayList<>();

        for(String topic: topicNames) {
            Result result = new Result();
            result.setBootstrap(kafkaStringURL);
            result.setClientId(clientID);
            result.setTopicName(topic);
            result.setKafkaErrorMessage(errorMessage);
            results.add(result);
            System.out.println(topic);
        }

        return results;
    }

    /**
     *
     * @param kafkaTopic
     * @return
     */
    private boolean isValidKafkaTopicName(String kafkaTopic) {


        return false;
    }

    private boolean isValidKafkaURL(String kafkaStringURL) {

        return false;
    }

    /**
     *
     * @param kafkaStringURL
     * @param kafkaTopic
     * @return
     */
    public Result deleteKafkaTopic(String kafkaStringURL, String kafkaTopic) {
        Result result = new Result();

        if ( kafkaStringURL == null || kafkaStringURL.trim().length()<=0 || kafkaTopic == null || kafkaTopic.trim().length()<=0) {
            return result;
        }

        try {
            String clientID = "nifi-" + RandomStringUtils.randomAlphabetic(25);
            AdminClient adminClient = createAdminClient(kafkaStringURL,clientID);
            Collection<String> topics = new ArrayList<>();
            topics.add(kafkaTopic);
            final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);

            try {
                deleteTopicsResult.all().get();

                result.setTopicName(kafkaTopic);

                adminClient.close();
                adminClient = null;
            } catch (InterruptedException | ExecutionException  e) {
                e.printStackTrace();
                result.setKafkaErrorMessage(e.getLocalizedMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
            result.setKafkaErrorMessage(e.getLocalizedMessage());
        }
        return result;
    }
}