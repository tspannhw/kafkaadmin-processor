package dev.datainmotion.kafka.kafkaadmin;

import java.io.Serializable;

/**
 *
 */
public class Result implements Serializable {

    private String kafkaTopicMessage = null;
    private String bootstrap = null;
    private String clientId = null;
    private String kafkaErrorMessage = null;

    /**
     *
     * @param kafkaTopicMessage
     * @param bootstrap
     * @param clientId
     * @param kafkaErrorMessage
     */
    public Result(String kafkaTopicMessage, String bootstrap, String clientId, String kafkaErrorMessage) {
        super();
        this.kafkaTopicMessage = kafkaTopicMessage;
        this.bootstrap = bootstrap;
        this.clientId = clientId;
        this.kafkaErrorMessage = kafkaErrorMessage;
    }

    /**
     *
     */
    public Result() {
        super();
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Result{");
        sb.append("kafkaTopicMessage='").append(kafkaTopicMessage).append('\'');
        sb.append(", bootstrap='").append(bootstrap).append('\'');
        sb.append(", clientId='").append(clientId).append('\'');
        sb.append(", kafkaErrorMessage='").append(kafkaErrorMessage).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public void setKafkaTopicMessage(String kafkaTopicMessage) {
        this.kafkaTopicMessage = kafkaTopicMessage;
    }

    public void setBootstrap(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setKafkaErrorMessage(String kafkaErrorMessage) {
        this.kafkaErrorMessage = kafkaErrorMessage;
    }

    public String getKafkaTopicMessage() {
        return kafkaTopicMessage;
    }

    public String getBootstrap() {
        return bootstrap;
    }

    public String getClientId() {
        return clientId;
    }

    public String getKafkaErrorMessage() {
        return kafkaErrorMessage;
    }
}
