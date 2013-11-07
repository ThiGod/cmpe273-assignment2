package edu.sjsu.cmpe.procurement.config;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.client.JerseyClientConfiguration;
import com.yammer.dropwizard.config.Configuration;

public class ProcurementServiceConfiguration extends Configuration {
    @NotEmpty
    @JsonProperty
    private String stompQueueName;

    @NotEmpty
    @JsonProperty
    private String stompTopicName;

    /*
    @Valid
    @NotNull
    @JsonProperty
    private JerseyClientConfiguration httpClient = new JerseyClientConfiguration();
	*/
    
    @NotEmpty
    @JsonProperty
    private String apolloUser;
    
    @NotEmpty
    @JsonProperty
    private String apolloPassword;
    
    @NotEmpty
    @JsonProperty
    private String apolloHost;
    
    @NotEmpty
    @JsonProperty
    private String apolloPort;
    
    /**
     * 
     * @return
     */
    /*
    public JerseyClientConfiguration getJerseyClientConfiguration() {
	return httpClient;
    }
    */

    /**
     * @return the stompQueueName
     */
    public String getStompQueueName() {
	return stompQueueName;
    }

    /**
     * @param stompQueueName
     *            the stompQueueName to set
     */
    public void setStompQueueName(String stompQueueName) {
	this.stompQueueName = stompQueueName;
    }

    public String getStompTopicName() {
	return stompTopicName;
    }

    public void setStompTopicName(String stompTopicName) {
	this.stompTopicName = stompTopicName;
    }

    /**
     * @return the apolloUser
     */
    public String getApolloUser() {
	return apolloUser;
    }

    /**
     * @param apolloUser
     *            the apolloUser to set
     */
    public void setApolloUser(String apolloUser) {
	this.apolloUser = apolloUser;
    }

    /**
     * @return the apolloPassword
     */
    public String getApolloPassword() {
	return apolloPassword;
    }

    /**
     * @param apolloPassword
     *            the apolloPassword to set
     */
    public void setApolloPassword(String apolloPassword) {
	this.apolloPassword = apolloPassword;
    }

    /**
     * @return the apolloHost
     */
    public String getApolloHost() {
	return apolloHost;
    }

    /**
     * @param apolloHost
     *            the apolloHost to set
     */
    public void setApolloHost(String apolloHost) {
	this.apolloHost = apolloHost;
    }

    /**
     * @return the apolloPort
     */
    public String getApolloPort() {
	return apolloPort;
    }

    /**
     * @param apolloPort
     *            the apolloPort to set
     */
    public void setApolloPort(String apolloPort) {
	this.apolloPort = apolloPort;
    }
}
