package edu.sjsu.cmpe.procurement.jobs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.ws.rs.core.MediaType;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.WebResource;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;

/**
 * This job will run at every 5 second.
 */
@Every("5min")
public class ProcurementSchedulerJob extends Job {
    private final Logger log = LoggerFactory.getLogger(getClass());
	
    @Override
    public void doJob() {
    	/*
	String strResponse = ProcurementService.jerseyClient.resource(
		"http://ip.jsontest.com/").get(String.class);
	log.debug("Response from jsontest.com: {}", strResponse);
	*/    	
	String user = env("APOLLO_USER", "admin");
	String password = env("APOLLO_PASSWORD", "password");
	String host = env("APOLLO_HOST", "54.215.210.214");
	int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
	
	try {
    	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
    	factory.setBrokerURI("tcp://" + host + ":" + port);

    	Connection connection = factory.createConnection(user, password);
    	String queueName = ProcurementService.queueName;
    	WebResource postPublisher = ProcurementService.postPublisher;
    	WebResource getPublisher = ProcurementService.getPublisher;
   	
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		
    	Destination destination = new StompJmsDestination(queueName);
    	MessageConsumer msgConsumer = session.createConsumer(destination);
    	ArrayList<String> topic = new ArrayList<String>();
    	ArrayList<String> isbnsArrayList = new ArrayList<String>();
    	
    	String topicQueue = "/topic/11830.book";
    	long waitUntil = 5000;
    	System.out.println("Waiting for messages from " + queueName + "...");
   
    	while(true) {
    	    Message msg = msgConsumer.receive(waitUntil);
    		    if( msg instanceof  TextMessage) {
    		    	String body = ((TextMessage) msg).getText();
    		    	System.out.println("Received TextMessage = " + body);
    		    	String parse[] = body.split("[:]");
    		    	isbnsArrayList.add(parse[1]);
    		    } 
    		    else if (msg == null) {
    		          System.out.println("No new messages. Existing due to timeout - " + waitUntil / 1000 + " sec");
    		          break;
    		    } 
    		    else {
    		         System.out.println("Unexpected message type: " + msg.getClass());
    		    }
    	}
    	
    	if (isbnsArrayList.size()!= 0){   		
			postHTTP(postPublisher, isbnsArrayList);
			topic = getHTTP(getPublisher);
			for (int i = 0 ; i<topic.size() ; i++){
				topicQueue = "/topic/11830.book";
				if (topic.get(i).toLowerCase().contains(":computer:")) {
					getSTOMP(topic.get(i),connection,topicQueue+".computer");
				} else if (topic.get(i).toLowerCase().contains(":comics:")) {
						getSTOMP(topic.get(i),connection,topicQueue+".comics");
				} else if (topic.get(i).toLowerCase().contains(":management:")) {
						getSTOMP(topic.get(i),connection,topicQueue+".management");
				} else if (topic.get(i).toLowerCase().contains(":selfimprovement:")) {
						getSTOMP(topic.get(i),connection,topicQueue+".selfimprovement");
				} else {
						getSTOMP(topic.get(i),connection,topicQueue);
				}
			}
    	}
    	connection.close();
    	System.out.println("Done");
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    private static String env(String key, String defaultValue) {
		String rc = System.getenv(key);
		if( rc== null ) {
			return defaultValue;
		}
		return rc;
	}
    
    public void getSTOMP(String message, Connection connection, String queue) throws JMSException{
		String destination = queue;
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(destination);
		MessageProducer producer = session.createProducer(dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

		System.out.println("Sending messages to " + queue + "..." + message);
		String data = message; 
		TextMessage msg = session.createTextMessage(data);
		msg.setLongProperty("id", System.currentTimeMillis());
		producer.send(msg);
    }
    
    private static void postHTTP(WebResource r,ArrayList<String> isbns){
    	HashMap<String, Serializable> request = new HashMap<String, Serializable>();
	    request.put("id", "11830");
	    request.put("order_book_isbns", isbns);
	    System.out.println(request);
	    String response = r.accept(
	        MediaType.APPLICATION_JSON_TYPE,
	        MediaType.APPLICATION_XML_TYPE).
	        header("X-FOO", "BAR").
	        entity(request, MediaType.APPLICATION_JSON_TYPE).
	        post(String.class);
	    System.out.println(response);
    }
    
    private static ArrayList<String> getHTTP(WebResource w){
    	HashMap items = new HashMap();
    	items = w.accept(MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_XML_TYPE).
    			header("X-FOO", "BAR").
    			type(MediaType.APPLICATION_JSON_TYPE).
    			get(HashMap.class);
    	ArrayList<HashMap> bookHashMap = new ArrayList<HashMap>();
    	ArrayList<String> finalUrl = new ArrayList<String>();
    	bookHashMap = (ArrayList<HashMap>) items.get("shipped_books");	
    	String rul;
    	for (int i= 0; i < bookHashMap.size(); i++ ){
    		rul = "";
    		rul = rul + bookHashMap.get(i).get("isbn")+":"+bookHashMap.get(i).get("title")
    				+":"+bookHashMap.get(i).get("category")+":"+bookHashMap.get(i).get("coverimage");
    		finalUrl.add(rul);
    	}
    	return finalUrl;
    }
}
