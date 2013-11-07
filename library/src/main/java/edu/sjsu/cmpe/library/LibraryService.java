package edu.sjsu.cmpe.library;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.assets.AssetsBundle;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.views.ViewBundle;

import edu.sjsu.cmpe.library.api.resources.BookResource;
import edu.sjsu.cmpe.library.api.resources.RootResource;
import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;
import edu.sjsu.cmpe.library.repository.BookRepository;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;
import edu.sjsu.cmpe.library.ui.resources.HomeResource;

public class LibraryService extends Service<LibraryServiceConfiguration> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) throws Exception {
	new LibraryService().run(args);
    }

    @Override
    public void initialize(Bootstrap<LibraryServiceConfiguration> bootstrap) {
	bootstrap.setName("library-service");
	bootstrap.addBundle(new ViewBundle());
	bootstrap.addBundle(new AssetsBundle());
    }

    @Override
    public void run(LibraryServiceConfiguration configuration,
	    Environment environment) throws Exception {
	// This is how you pull the configurations from library_x_config.yml
	String libraryName = configuration.getLibraryName();
	String apolloUser = configuration.getApolloUser();
	String apolloPwd = configuration.getApolloPassword();
	String apolloHost = configuration.getApolloHost();
	String apolloPort = configuration.getApolloPort();
	String queueName = configuration.getStompQueueName();
	final String topicName = configuration.getStompTopicName();
	final BookRepositoryInterface bookRepository;
	final Connection connection;

	log.debug("{} - Queue name is {}. Topic name is {}",
		configuration.getLibraryName(), queueName,
		topicName);
	// TODO: Apollo STOMP Broker URL and login
	String user = env("APOLLO_USER", apolloUser);
	String password = env("APOLLO_PASSWORD", apolloPwd);
	String host = env("APOLLO_HOST", apolloHost);
	int port = Integer.parseInt(env("APOLLO_PORT", apolloPort));

	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
	factory.setBrokerURI("tcp://" + host + ":" + port);

	connection = factory.createConnection(user, password);
	connection.start();
	bookRepository = new BookRepository();
	
	/** Root API */
	environment.addResource(RootResource.class);
	/** Books APIs */
	environment.addResource(new BookResource(bookRepository, connection, queueName, libraryName));
	
	int numThreads = 1;
	ExecutorService executor = Executors.newFixedThreadPool(numThreads);
	Runnable backgroundTask = new Runnable() {
		@Override
		public void run() {
			Producer produce = new Producer (bookRepository, connection, topicName);
			try {
				produce.produce();
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	};
	executor.execute(backgroundTask);
	executor.shutdown();
	
	/** UI Resources */
	environment.addResource(new HomeResource(bookRepository));
    }
    
    private static String env(String key, String defaultValue) {
        String rc = System.getenv(key);
        if( rc== null ) {
            return defaultValue;
        }
        return rc;
    }
    
    /*
    private static String arg(String []args, int index, String defaultValue) 
	{
		if( index < args.length ) {
			return args[index];
		} else {
			return defaultValue;
		}
	}
	*/
}
