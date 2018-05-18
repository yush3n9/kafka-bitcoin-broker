package yusheng.demo;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SampleProducer extends Thread {
	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	private final Boolean isAsync;
	private final int interval;

	public static final String KAFKA_SERVER_URL = "localhost";
	public static final int KAFKA_SERVER_PORT = 9092;
	public static final String CLIENT_ID = "SampleProducer";

	public SampleProducer(String topic, Boolean isAsync, int interval) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
		properties.put("client.id", CLIENT_ID);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<Integer, String>(properties);
		this.topic = topic;
		this.isAsync = isAsync;
		this.interval = interval;
	}

	public void run() {
		int messageNo = 1;
		while (true) {
			long startTime = System.currentTimeMillis();
			try {
				sleep(this.interval);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			Client client = ClientBuilder.newClient();
			WebTarget target = client.target("https://blockchain.info/de/ticker");
			Response response = target.request(MediaType.APPLICATION_JSON).get();
			String charts = response.readEntity(String.class);

			if (isAsync) { // Send asynchronously
				producer.send(new ProducerRecord<>(topic, messageNo, charts),
						new DemoCallBack(startTime, messageNo, charts));
			} else { // Send synchronously
				try {
					producer.send(new ProducerRecord<>(topic, messageNo, charts)).get();
					System.out.println("Sent message: (" + messageNo + ", " + charts + ")");
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
					// handle the exception
				}
			}
			++messageNo;
		}
	}
}

class DemoCallBack implements Callback {

	private final long startTime;
	private final int key;
	private final String message;

	public DemoCallBack(long startTime, int key, String message) {
		this.startTime = startTime;
		this.key = key;
		this.message = message;
	}

	/**
	 * onCompletion method will be called when the record sent to the Kafka Server
	 * has been acknowledged.
	 * 
	 * @param metadata
	 *            The metadata contains the partition and offset of the record. Null
	 *            if an error occurred.
	 * @param exception
	 *            The exception thrown during processing of this record. Null if no
	 *            error occurred.
	 */
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		if (metadata != null) {
			System.out.println("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), "
					+ "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
		} else {
			exception.printStackTrace();
		}
	}
}