import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonParser;
import com.google.gson.Gson;
import kafka.utils.json.JsonObject;

public class SimpleProducer {
	Properties props;
	// The producer is a Kafka client that publishes records to the Kafka
	// cluster.
	KafkaProducer<String, String> producer;
	
	SimpleProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		// Serializer for conversion the key type to bytes
		props.put("key.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		// Serializer for conversion the value type to bytes
		props.put("value.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
	}
	void produceAndPrint() {
		for (int i = 1; i < 100; i++)
			// Fire-and-forget send(topic, key, value)
			// Send adds records to unsent records buffer and return
			producer.send(new ProducerRecord<String, String>("SDTF", Integer
			.toString(i), Integer.toString(i)));
	}
	void stop() {
		System.out.printf("End producer");
		producer.close();
	}
	
	public static void main (String[] args) {
//		System.setProperty("kafka.logs.dir", "/home/aron/Desktop/kafka/logs");
//		System.getProperties().list(System.out);
		
		SimpleProducer myProducer = new SimpleProducer();
		myProducer.produceAndPrint();
		myProducer.stop();
	}
	
	
	
}
