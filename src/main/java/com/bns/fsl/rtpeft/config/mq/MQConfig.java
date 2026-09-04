package com.bns.fsl.rtpeft.config.mq;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.connection.CachingConnectionFactory;

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

/**
 * IBM MQ connection used for two independent queues:
 *  - ACI request queue (this service publishes, ACI/Fraud Decision Service consumes)
 *  - NRT audit queue (this service publishes, downstream audit/NRT consumers listen)
 *
 * ACI's response queue is intentionally NOT wired here - it is consumed
 * exclusively by Fraud Decision Service, which this service never touches.
 */
@Configuration
public class MQConfig {

    @Value("${fsl.mq.host}")
    private String host;
    @Value("${fsl.mq.port}")
    private int port;
    @Value("${fsl.mq.channel}")
    private String channel;
    @Value("${fsl.mq.queue-manager}")
    private String queueManager;
    @Value("${fsl.mq.username}")
    private String username;
    @Value("${fsl.mq.password}")
    private String password;

    @Bean
    public MQConnectionFactory mqConnectionFactory() throws Exception {
        MQConnectionFactory factory = new MQConnectionFactory();
        factory.setHostName(host);
        factory.setPort(port);
        factory.setChannel(channel);
        factory.setQueueManager(queueManager);
        factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        return factory;
    }

    @Bean
    public JmsTemplate jmsTemplate(MQConnectionFactory mqConnectionFactory) {
        CachingConnectionFactory cachingFactory = new CachingConnectionFactory(mqConnectionFactory);
        cachingFactory.setUsername(username);
        cachingFactory.setPassword(password);
        JmsTemplate template = new JmsTemplate(cachingFactory);
        template.setDeliveryPersistent(true);
        return template;
    }
}
