import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;
import kafka.utils.json.JsonObject;

public class SimpleProducer {
	Properties props;
	// The producer is a Kafka client that publishes records to the Kafka
	// cluster.
	KafkaProducer<String, Data> producer;
	
	SimpleProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		// Serializer for conversion the key type to bytes
		props.put("key.serializer",
		"org.apache.kafka.common.serialization.StringSerializer");
		// Serializer for conversion the value type to bytes
		props.put("value.serializer","UserSerializer");
		producer = new KafkaProducer<>(props);
	}
	void produceAndPrint() {
		for (int i = 1; i < 100; i++) {
		// Fire-and-forget send(topic, key, value)
		// Send adds records to unsent records buffer and return
		Data data=new Data(Integer.toString(i));
		producer.send(new ProducerRecord<String, Data>("SDTF2", Integer.toString(i), data));
		System.out.println("Sent "+data.toString());
		}
		
	}
	void stop() {
		System.out.printf("End producer");
		producer.close();
	}
	
	public static void main (String[] args) {
//		System.setProperty("kafka.logs.dir", "/home/aron/Desktop/kafka/logs");
//		System.getProperties().list(System.out);
//		URL url;
//		try {
//			url = new URL ("www.aemet.es/xml/municipios/localidad_28079.xml");
//			getInfo(url);
//		} catch (MalformedURLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		SimpleProducer myProducer = new SimpleProducer();
		myProducer.produceAndPrint();
		myProducer.stop();
	}
	
	public static String getInfo(URL url) {
		String output = null;
		try {
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			//conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Accept", "text/xml");
			if(conn.getResponseCode()!=200) {
				throw new RuntimeException("Failed : HTPP error code: " + conn.getResponseCode());
			}
			BufferedReader br = new BufferedReader (new InputStreamReader(conn.getInputStream()));
			StringBuilder sb = new StringBuilder();
			int cp;
			while((cp = br.read())!= -1) {
				sb.append((char)cp);
			}
			output = sb.toString();
			Gson g = new Gson();
			
			
//			JsonObject json = new Gson().fromJson(output, JsonObject.class);
//			System.out.println("Salida como JSON: " +json);
			System.out.println("Salida como String: "+output);
			
			conn.disconnect();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return output; 
	}
	
}
