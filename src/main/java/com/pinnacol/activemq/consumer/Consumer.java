package com.pinnacol.activemq.consumer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;



public class Consumer {
	
	
	private static String url = ActiveMQConnection.DEFAULT_BROKER_URL;
	
	private static String zedqueue1 = "zedqueue1";
	
	
	public static void main(String[] args) throws JMSException {
		
		boolean listening = true;
		
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
		
		Connection connection = connectionFactory.createConnection();
		
		connection.start();
		
		System.out.println("connection to activemq jms broker made ...");
		
		
		//create session for sending a message
		Session msgSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		
		Destination destination = msgSession.createQueue(zedqueue1);
		
		MessageConsumer consumer = msgSession.createConsumer(destination);
		
		
		while(listening) {
			
			Message message = consumer.receive();
			
			if (message instanceof TextMessage) {

				TextMessage textMessage = (TextMessage) message;
				System.out.println("Received message '" + textMessage.getText() + "'");
				
				if(textMessage.getText().equals("quit")) {
					listening = false;
				}
			}
			
		}
		
		//wait to receive a message from the queue
		

		
		connection.close();
		
		System.out.println("closed connection to activemq jms broker ...");
		
	}

}
