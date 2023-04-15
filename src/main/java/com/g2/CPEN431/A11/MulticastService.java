package com.g2.CPEN431.A11;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import java.util.*;

public class MulticastService {
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String TOPIC_NAME = "multicastTopic";

    public static void multicast(List<String> serverIps, List<Integer> serverPorts, byte[] data) throws JMSException {
        // Create a connection factory
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);

        // Create a connection
        Connection connection = connectionFactory.createConnection();
        connection.start();

        // Create a session
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the multicast topic
        Topic topic = session.createTopic(TOPIC_NAME);

        // Create the message producer
        MessageProducer producer = session.createProducer(topic);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);



        for (int i=0; i < serverIps.size(); i++){
            String serverIp = serverIps.get(i);
            int serverPort = serverPorts.get(i);

            String serverTopicName = serverIp + ":" + serverPort;
            Destination destination = session.createTopic(serverTopicName);
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(data);
            producer.send(destination, message);
        }

        // Clean up resources
        session.close();
        connection.close();
    }

}
