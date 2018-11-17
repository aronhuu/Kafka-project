
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jfree.data.category.DefaultCategoryDataset;

public class SimpleConsumer implements Runnable{
	KafkaConsumer <String, Weather> consumer;
	List <String> topics;
	List <DefaultCategoryDataset> database;
	boolean COMMENT =false;

	SimpleConsumer (List<String> topic_name, List <DefaultCategoryDataset> data){
		Properties props = new Properties();
		this.database=data;
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Group1");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"UserDeserializer");	
		consumer = new KafkaConsumer<>(props);
		this.topics= topic_name;
	}
	
	void subscribeAndConsume() {
		consumer.subscribe(topics);
//		double start=System.currentTimeMillis();
		while (true) {
			ConsumerRecords<String, Weather> records = consumer.poll(100);
			for (ConsumerRecord<String, Weather> record : records) { 
				Weather w = record.value();
				if(COMMENT) {
					System.out.println("something consumed");
					long threadID = Thread.currentThread().getId();
					System.out.println("->I AM THE THE THREAD N: " + threadID);
					System.out.println("Received "+w.getCity()+":"+w.getMax()+"-"+w.getMin());
					System.out.printf("offset = %d, key = %s, partition = %s, value = %s%n",
							record.offset(), record.key(), record.partition(), record.value());
				}
				if(w.getCity().isEmpty()) {
//					double stop =System.currentTimeMillis()-start;
					kafka_system.lastRecord(record.topic(),record.value().getMax(),System.currentTimeMillis());
					
				
				}else {
					if(database!=null)
						for (int i=0; i<topics.size() ; i++) {
							if(w.getCcaa().equals(topics.get(i))) {
//						if(w.getCcaa().equals(topics)) {
								database.get(i).setValue(w.getMax(), "Maximum temperature", w.getCity());
								database.get(i).setValue(w.getMin(), "Minimum temperature", w.getCity());			
							}
						}
					
				}

			}//for
		} //while
	}
	
	void stop() {
		System.out.printf("End consumer");
		consumer.close();
	}
	
//	
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
////		System.setProperty("kafka.logs.dir", "/home/aron/Desktop/kafka/logs");
////		System.getProperties().list(System.out);
//
//		
//		SimpleConsumer myConsumer = new SimpleConsumer("");
//		
//		myConsumer.subscribe(topics);
//		myConsumer.consume();
//		myConsumer.stop();
//
//	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

		subscribeAndConsume();
		stop();
		
	}

}
