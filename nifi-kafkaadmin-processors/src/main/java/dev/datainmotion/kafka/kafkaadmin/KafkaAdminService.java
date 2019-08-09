package dev.datainmotion.kafka.kafkaadmin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 */
public class KafkaAdminService {
    public KafkaAdminService() {
        super();
    }

    /**
     * @param properties
     */
    private String printTopicDetails(Properties properties) {
        StringBuilder out = new StringBuilder(128);
        Collection<TopicListing> listings = null;
        // Create  an AdminClient using the properties initialized earlier
        try (AdminClient client = AdminClient.create(properties)) {
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(false);
            listings = client.listTopics(options).listings().get();

            listings.forEach(
                    topic -> out.append("Name: ").append(topic.name()));
        } catch (Throwable t) {
            System.out.println("Error");
        }

        return out.toString();
    }

    /**
     * @param properties
     */
    private String printTopicDescription(Properties properties) {
        StringBuilder out = new StringBuilder(128);
        Collection<TopicListing> listings = null;
        // Create  an AdminClient using the properties initialized earlier
        try (AdminClient client = AdminClient.create(properties)) {
            listings = getTopicListing(client, false);
            List<String> topics = listings.stream().map(TopicListing::name)
                    .collect(Collectors.toList());
            DescribeTopicsResult result = client.describeTopics(topics);
            result.values().forEach((key, value) -> {
                try {
                    out.append(key).append(": ").append(value.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    System.out.println("Error");
                }
            });
        } catch (Throwable e) {
            System.out.println("Error");
        }

        return out.toString();
    }

    /**
     * @param kafkaURL
     * @return String
     */
    public String listKafkaTopics(String kafkaURL) {
        // First we need to initialize Kafka properties
        StringBuilder out = new StringBuilder(256);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaURL);
        properties.put("client.id", "nifi-admin-client");
        //out.append(printTopicDetails(properties));
        //out.append(printTopicDescription(properties));
        out.append(createTopic(kafkaURL,"test1", properties));

        out.append(getTopicNames(kafkaURL, properties).toString());
        return out.toString();
    }


    /**
     *
     * @param kafkaURL
     * @param properties
     * @return
     * @throws Exception
     */
    private Collection<String> getTopicNames(String kafkaURL, Properties properties) {
        Collection<String> allTopics = null;
        try {
            AdminClient adminClient = AdminClient.create(properties);
            allTopics = new TreeSet<>(
                    adminClient.listTopics().names().get(10, TimeUnit.MILLISECONDS));

            System.out.println("All topics:" +        allTopics.toString());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (java.util.concurrent.TimeoutException e) {
            e.printStackTrace();
        }

        return allTopics;
    }

    /**
     * create a topic in kafka
     * @param kafkaURL
     * @param topicName
     * @param properties
     * @return
     */
    private String createTopic(String kafkaURL, String topicName, Properties properties) {

        StringBuilder out = new StringBuilder(128);
        AdminClient adminClient = AdminClient.create(properties);

        CreateTopicsResult topicResults = null;

        try {
            List<NewTopic> topicsToAdd = new ArrayList<>();
            NewTopic newTopic = null;
            newTopic = new NewTopic(topicName, 1, (short) 1);
            topicsToAdd.add(newTopic);
            topicResults = adminClient.createTopics(topicsToAdd);
            topicResults.all().get( 30, TimeUnit.SECONDS);

            out.append(topicResults.toString());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
        catch (TimeoutException e) {
            e.printStackTrace();
        }
        catch (Exception e) {
            if (e.getCause() instanceof TopicExistsException) { // Possible race with another app instance
                e.printStackTrace();
                out.append(e.getLocalizedMessage());
            }
            else {
                out.append("Other topic error");
            }
        }

        out.append("Created new topic: ").append(topicName);
        return out.toString();
    }

    /**
     * @param client
     * @param isInternal
     * @return
     */
    private Collection<TopicListing> getTopicListing(AdminClient client, boolean isInternal) {
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(isInternal);
        Collection<TopicListing> topicListing = null;
        try {
            topicListing = client.listTopics(options).listings().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return topicListing;
    }

    /**
     *
     * @param kafkaStringURL
     * @param kafkaTopic
     * @return String
     */
    public String createKafkaTopic(String kafkaStringURL, String kafkaTopic) {
        // First we need to initialize Kafka properties
        StringBuilder out = new StringBuilder(256);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaStringURL);
        properties.put("client.id", "nifi-admin-client");
        out.append(properties.toString());
        //RandomStringUtils.randomAlphabetic(10);

        out.append(createTopic(kafkaStringURL,kafkaTopic, properties));
        return out.toString();
    }
}