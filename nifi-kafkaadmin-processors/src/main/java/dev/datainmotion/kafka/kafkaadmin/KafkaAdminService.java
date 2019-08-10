package dev.datainmotion.kafka.kafkaadmin;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TimeoutException;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

/**
 *
 */
public class KafkaAdminService {

    public static final String INVALID_KAFKA_URL_OR_TOPIC_NAME = "Invalid Kafka URL or Topic Name";
    public static final String INVALID_KAFKA_URL = "Invalid Kafka URL";

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
        // Must be a valid topic name and valid URL
        Result result = new Result();
        if (!isValidKafkaURL(kafkaURL) || !isValidKafkaTopicName(topicName) || clientID == null ||
                clientID.trim().length() <=0) {
            result.setKafkaErrorMessage(INVALID_KAFKA_URL_OR_TOPIC_NAME);
            return result;
        }

        CreateTopicsResult topicResults = null;
        boolean createdTopic = true;

        try {
            AdminClient adminClient = createAdminClient(kafkaURL, clientID);
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
        } else {
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
        // Must be a valid topic name and valid URL
        if (!isValidKafkaURL(kafkaStringURL) || !isValidKafkaTopicName(kafkaTopic)) {
            Result result = new Result();
            result.setKafkaErrorMessage(INVALID_KAFKA_URL_OR_TOPIC_NAME);
            return result;
        }
        String clientID = "nifi-" + RandomStringUtils.randomAlphabetic(25);
        Result topicOutput = createTopic(kafkaStringURL, kafkaTopic.trim(), clientID);
        topicOutput.setBootstrap(kafkaStringURL);
        topicOutput.setClientId(clientID);
        return topicOutput;
    }


    /**
     * create admin client
     *
     * @param kafkaStringURL
     * @return
     */
    private AdminClient createAdminClient(String kafkaStringURL, String clientID) {
        // Must be a valid URL
        if (!isValidKafkaURL(kafkaStringURL)) {
            System.err.println(INVALID_KAFKA_URL);
            return null;
        }
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaStringURL);
        properties.put("client.id", clientID);
        AdminClient adminClient = null;

        try {
            adminClient = AdminClient.create(properties);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return adminClient;
    }

    /**
     * list kafka topics
     *
     * @param kafkaStringURL
     * @return list of kafka topics
     */
    public List<Result> listKafkaTopics(String kafkaStringURL) {
        List<Result> results = new ArrayList<>();

        // Must be a valid URL
        if (!isValidKafkaURL(kafkaStringURL)) {
            Result errorResult = new Result();
            errorResult.setKafkaErrorMessage(INVALID_KAFKA_URL);
            results.add(errorResult);
            return results;
        }

        String clientID = "nifi-" + RandomStringUtils.randomAlphabetic(25);
        AdminClient adminClient = createAdminClient(kafkaStringURL, clientID);
        ListTopicsResult topics = null;

        try {
            topics = adminClient.listTopics();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Set<String> topicNames = null;
        String errorMessage = null;
        try {
            if ( topics != null ) {
                topicNames = topics.names().get();
            }
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
            if (adminClient != null) {
                adminClient.close();
            }
            adminClient = null;
        } catch (Exception e) {
            e.printStackTrace();
            errorMessage = e.getLocalizedMessage();
        }

        try {
            for (String topic : topicNames) {
                Result result = new Result();
                result.setBootstrap(kafkaStringURL);
                result.setClientId(clientID);
                result.setTopicName(topic);
                result.setKafkaErrorMessage(errorMessage);
                results.add(result);
                System.out.println(topic);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }

    /**
     * topic names cannot be longer than 249 characters.
     * https://riccomini.name/how-paint-bike-shed-kafka-topic-naming-conventions?source=post_page-----1b7259790073----------------------
     * Valid characters for Kafka topics are the ASCII alphanumerics, ‘.’, ‘_’, and ‘-‘
     *
     * @param kafkaTopic
     * @return true / false
     */
    private boolean isValidKafkaTopicName(String kafkaTopic) {

        if (kafkaTopic == null || kafkaTopic.trim().length() <= 0){
            return false;
        }

        if (kafkaTopic.length() > 249) {
            return false;
        }

        if (!kafkaTopic.matches("^[a-zA-Z0-9._-]*$")) {
            return false;
        }

        return true;
    }

    /**
     * check for valid ipv4/dns (upgrade to ipv6 at some point)
     * https://stackoverflow.com/questions/36247902/java-regex-string-check-if-ipport-string-contains-a-valid-ipv4-or-dns-address
     *
     * @param kafkaStringURL
     * @return true/false
     */
    private boolean isValidKafkaURL(String kafkaStringURL) {
        if (kafkaStringURL == null || kafkaStringURL.trim().length() <= 0) {
            return false;
        }

        Pattern p = null;
        boolean isValid = false;

        try {
            p = Pattern.compile("^"
                    + "(((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}" // Domain name
                    + "|"
                    + "localhost" // localhost
                    + "|"
                    + "(([0-9]{1,3}\\.){3})[0-9]{1,3})" // Ip
                    + ":"
                    + "[0-9]{1,5}$"); // Port

            isValid = p.matcher(kafkaStringURL).matches();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return isValid;
    }

    /**
     * @param kafkaStringURL
     * @param kafkaTopic
     * @return
     */
    public Result deleteKafkaTopic(String kafkaStringURL, String kafkaTopic) {
        Result result = new Result();

        // Must be a valid topic name and valid URL
        if (!isValidKafkaURL(kafkaStringURL) || !isValidKafkaTopicName(kafkaTopic)) {
            result.setKafkaErrorMessage(INVALID_KAFKA_URL_OR_TOPIC_NAME);
            return result;
        }

        try {
            String clientID = "nifi-" + RandomStringUtils.randomAlphabetic(25);
            AdminClient adminClient = createAdminClient(kafkaStringURL, clientID);
            if ( adminClient == null) {
                result.setKafkaErrorMessage(INVALID_KAFKA_URL_OR_TOPIC_NAME);
                return result;
            }
            Collection<String> topics = new ArrayList<>();
            topics.add(kafkaTopic);
            final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);

            try {
                deleteTopicsResult.all().get();

                result.setTopicName(kafkaTopic);

                if ( adminClient != null) {
                    adminClient.close();
                }
                adminClient = null;
            } catch (InterruptedException | ExecutionException e) {
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