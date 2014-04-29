package com.pinnacol.activemq.consumer;

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

public class TestConsumeJMSHeaderInfo {
	
	private static String url = ActiveMQConnection.DEFAULT_BROKER_URL;	
	private static String queueHeader = "testHeaderQueue";
	
	private ConnectionFactory connectionFactory;
	private Connection connection;
	private Session queueSession;
	private Destination destination;
	private MessageConsumer consumer;
	private boolean listening;
	
	private int messageCount = 10000;
	
	
	public TestConsumeJMSHeaderInfo() throws JMSException {
		
		this.connectionFactory = new ActiveMQConnectionFactory(url);
		this.connection = this.connectionFactory.createConnection();
		
		this.connection.start();
		
		this.listening = true;
		
	}
	
	public static void main(String[] args) throws JMSException {
		
		System.out.println("Calling TestConsumeJMSHeaderInfo application");
		
		TestConsumeJMSHeaderInfo consumerApp = new TestConsumeJMSHeaderInfo();
		
		consumerApp.createSessionToReceieveMessages();
		
		consumerApp.consumeAndDisplayMessages();
		
		
		System.out.println("closing connection to queue " + queueHeader);
		
		consumerApp.close();
		
		
	}
	
	protected void createSessionToReceieveMessages() throws JMSException {
		
		//create the session on which to consume messages from the queue
		this.queueSession = this.connection.createSession(false,  Session.AUTO_ACKNOWLEDGE);
		
		//this will create the queue if one does not exist - otherwise it will register with the queue
		this.destination = this.queueSession.createQueue(queueHeader);
		
		//create the consumer of the messages for that queue
		this.consumer = this.queueSession.createConsumer(destination);
		
	}
	
	
	protected void consumeAndDisplayMessages() throws JMSException {
		
		StopWatch sw = new StopWatch();
		int count = 0;
		
		sw.start();
		
		while(this.listening) {
			
			count++;
			
			System.out.println("count = " + count);

			Message message = this.consumer.receive();

			Enumeration propertyNames = message.getPropertyNames();

			while(propertyNames.hasMoreElements()) {

				String name = (String) propertyNames.nextElement();

				System.out.println("propertyname = " + name);

				Object value = message.getObjectProperty(name);

				System.out.println("value = " + value);

			}
			
			if(count == this.messageCount) {
				this.listening = false;
			}
			
		}
		
		sw.stop();
		
		System.out.println("Time to consume " + count + " messages is " + sw.getElapsedTime() + " ms");
		
	}
	
	protected void close() throws JMSException {
		this.connection.close();
	}

}
