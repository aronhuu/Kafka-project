import java.util.Date;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


//import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;

import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.io.File;
import java.io.InputStream;



public class SimpleProducer {
		
	final static int NUM_CITIES = 200;

	Properties props;
	String [] ccaas;
	String [] cities = new String[NUM_CITIES];
	String [] tempMax = new String[NUM_CITIES];
	String [] tempMin = new String[NUM_CITIES];
	
	
	// The producer is a Kafka client that publishes records to the Kafka
	// cluster.
	KafkaProducer<String, Weather> producer;
	
	
	SimpleProducer(String [] ccaas) {
		this.ccaas=ccaas;
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		// Serializer for conversion the key type to bytes
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// Serializer for conversion the value type to bytes
		props.put("value.serializer", "UserSerializer");
		producer = new KafkaProducer<>(props);
	}
	
	void produceAndPrint() {
		
		parser();
//		for (int i = 0; i < cities.length; i++) {
//		// Fire-and-forget send(topic, key, value)
//		// Send adds records to unsent records buffer and return
//		System.out.println(cities[i]);
//		producer.send(new ProducerRecord<String, Weather>("TEMPERATURES", "temperature-max", cities[i] + "_" +tempMax[i] ));
//		}
//		
//		for (int i = 0; i < cities.length; i++) {
//			// Fire-and-forget send(topic, key, value)
//			// Send adds records to unsent records buffer and return
//			System.out.println(cities[i]);
//			producer.send(new ProducerRecord<String, Weather>("TEMPERATURES", "temperature-min", cities[i] + "_" +tempMin[i] ));
//			}
	}
	
	
	void stop() {
		System.out.println("end producer");
		producer.close();
		
	}
	
	
	void parser() {
		int counter=0;
		String contentType = "";
		HttpURLConnection urlConnection = null;
		
		try {
			
			//NEW
			URL url = new URL ("http://www.aemet.es/xml/ccaa/"+new SimpleDateFormat("yyyyMMdd").format(new Date())+"_t_prev_esp.xml");
			urlConnection = (HttpURLConnection)url.openConnection();
			contentType = urlConnection.getContentType();
			InputStream is = urlConnection.getInputStream();
			
			if (contentType.contains("xml")) {
				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	            factory.setExpandEntityReferences(false);
	            factory.setIgnoringComments(true);
	            factory.setIgnoringElementContentWhitespace(true);
	            DocumentBuilder builder = factory.newDocumentBuilder();
	            Document doc = builder.parse(is);
//	            if(true) {
//				//OLD
//		         File inputFile = new File("20181109_t_prev_esp.xml");
//		         DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
//		         DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
//		         Document doc = dBuilder.parse(inputFile);
//		         doc.getDocumentElement().normalize();
//		         System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
	            
				 for (int l = 0; l<ccaas.length;l++){
		         NodeList ccaaList = doc.getElementsByTagName("ccaa");
		         //System.out.println("----------------------------");
		         
		         for (int i = 0; i < ccaaList.getLength(); i++) {
		        	 Element ccaa = (Element) ccaaList.item(i);
					//System.out.println("\nCOMUNIDAD AUT�NOMA: " + ccaa.getAttribute("nombre"));
		        	 	String wCcaa=ccaa.getAttribute("nombre").trim();
						 if(wCcaa.replace(" ","_").equals(ccaas[l])){
							System.out.println("\nCOMUNIDAD AUT�NOMA: " + ccaa.getAttribute("nombre"));

							//System.out.println("\nCOMUNIDAD AUT�NOMA: " + ccaa.getAttribute("nombre"));
						 
							//Entrar a las provincias
							NodeList provinceList = ccaa.getElementsByTagName("provincia");
							
							for (int j = 0; j < provinceList.getLength(); j++) {
								Element provincia = (Element) provinceList.item(j);
								//System.out.println("\n*PROVINCIA: " + provincia.getAttribute("nombre"));
								String wProvince=provincia.getAttribute("nombre");
								//Entrar a ciudades
								 NodeList ciudadesList = provincia.getElementsByTagName("ciudad");
								
								 for(int k = 0; k < ciudadesList.getLength() ; k++) {
									Element ciudad = (Element) ciudadesList.item(k);
									//System.out.println("\n>>>CIUDAD: " + ciudad.getAttribute("nombre"));
									cities[counter] = ciudad.getAttribute("nombre");
									String wCity = ciudad.getAttribute("nombre");
									
									
									//Recover tmax
									NodeList tmaxList = ciudad.getElementsByTagName("tmax");
									Element tmax = (Element) tmaxList.item(0); // There is just one description item
									tempMax[counter] = tmax.getTextContent().trim();
									//System.out.println("\n-------TMAX: " + tmax.getTextContent().trim());
									double wTmax= Double.parseDouble(tmax.getTextContent().trim());
									
									//Recover tmin
									NodeList tminList = ciudad.getElementsByTagName("tmin");
									Element tmin = (Element) tminList.item(0); // There is just one description item
									//System.out.println("\n-------TMIN: " + tmin.getTextContent().trim());
									tempMin[counter] = tmin.getTextContent().trim();
									double wTmin= Double.parseDouble(tmin.getTextContent().trim());

									counter++;
									
									//producer send record
									producer.send(new ProducerRecord<String, Weather>(ccaas[l],Integer.toString(counter), new Weather(ccaas[l],wProvince,wCity,wTmax,wTmin)));
									System.out.println(wCcaa+":"+wProvince+":"+wCity+":"+wTmax+":"+ wTmin);
									
								 }//for
							}//for province
		                }//if 
					 }
					producer.send(new ProducerRecord<String, Weather>(ccaas[l],Integer.toString(counter+1), new Weather("","","",0,0)));
				}
				 
			 }
	      	} catch (Exception e) {
	         e.printStackTrace();
	      }


	}
	
}