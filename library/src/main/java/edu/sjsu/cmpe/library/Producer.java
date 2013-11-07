package edu.sjsu.cmpe.library;

import java.net.MalformedURLException;
import java.net.URL;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsDestination;

import edu.sjsu.cmpe.library.domain.Book;
import edu.sjsu.cmpe.library.domain.Book.Status;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;

public class Producer {
	private BookRepositoryInterface bookRepository;
	private String topicName;
	private Connection connection;

	public Producer(BookRepositoryInterface bookRepository, 
			Connection connection, String topicName) {
		this.bookRepository = bookRepository;
		this.connection = connection;
		this.topicName = topicName;
	}

	public void produce() throws JMSException {
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(topicName);
		MessageConsumer consumer = session.createConsumer(dest);
		System.currentTimeMillis();
		System.out.println("Waiting for messages from " + topicName + "...");
		while(true) {
			Message msg = consumer.receive(5000);
			if(msg == null){	
				continue;
			}	
			else if( msg instanceof  TextMessage ) {
				String body = ((TextMessage) msg).getText();
				System.out.println("Received message = " + body);
				update(body);
			} else {
				System.out.println("Unexpected message type: " + msg.getClass());
			}
		}
	}
	
	public void update(String line) {
		line = line.replaceAll("\"", "");
		String[] tokens = line.split(":");
		long isbn = Long.parseLong(tokens[0]);
		String title = tokens[1];
		String category = tokens[2];
		String image = tokens[3] + ":" + tokens[4];
		Book book = bookRepository.getBookByISBN(isbn);	
		if(book == null) {
			Book newBook = new Book();
			newBook.setIsbn(isbn);
			newBook.setTitle(title);
			newBook.setCategory(category);	
			try {
				newBook.setCoverimage(new URL(image));
			} catch (MalformedURLException e) {
			    e.printStackTrace();
			}		
			bookRepository.saveBook(newBook);
		}	
		else {
			 book.setStatus(Status.available);
		}
	}
}
