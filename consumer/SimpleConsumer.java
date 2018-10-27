import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleConsumer {
	KafkaConsumer <String, String> consumer;

	SimpleConsumer (){
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Group1");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		
		consumer = new KafkaConsumer<>(props);
	}
	
	void subscribe(List<String> topics) {
		consumer.subscribe(topics);
	}
	
	void consume() {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) { 
				System.out.println("something consumed");
				System.out.printf("offset = %d, key = %s, key = %s, value = %s%n",
				record.offset(), record.key(), record.partition(), record.value());
			}//for
		} //while
	}
	
	void stop() {
		System.out.printf("End consumer");
		consumer.close();
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		System.setProperty("kafka.logs.dir", "/home/aron/Desktop/kafka/logs");
//		System.getProperties().list(System.out);

		List <String> topics = Arrays.asList("SDTF");
		
		SimpleConsumer myConsumer = new SimpleConsumer();
		
		myConsumer.subscribe(topics);
		myConsumer.consume();
		myConsumer.stop();

	}


}
