import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.swing.JFrame;
import javax.swing.JPanel;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;


public class kafka_system {
	
	final static int NUMBER_OF_THREADS = 2;	//Number of threads per consumer
	final static boolean ALL = true;		//If all the ccaas' weather information are produced
	final static boolean GRAPH = false;		//If showing the graph of temperatures
	final static boolean SUBSCRIBE_ALL_TOPICS = false;		//If the consumer subscribes all the topics
	//The number of consumers depends on the NUMBER_OF_THREADS and the type of subscription
	//If SUBSCRIBE_ALL_TOPICS is true, the consumer consumes all the topics
	//	And the total number of consumers is NUMBER_OF_THREADS
	//Otherwise, each consumer consumes 1 topic only													
	//	The total number of consumers is NUMBER_OF_THREADS * NUMBER OF TOPICS

	static double start;
	public static void main(String[] args) {
		
		List <String> topic_name_id = null;
		try {
			 topic_name_id = readConfig("topic.conf", ALL);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		//The topics don't allow spaces between words, underscore needed
		
		List <DefaultCategoryDataset> data=new ArrayList<DefaultCategoryDataset>();;
		
		List <String> topic_name= new ArrayList<String>();
		for (int i=0; i<topic_name_id.size();i++) {
			if(GRAPH)
				data.add(createChart("Weather in "+topic_name_id.get(i).split(",")[0]));//,createChart("Weather in Madrid")
			topic_name.add(topic_name_id.get(i).split(",")[0]);
		}

		ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS*topic_name.size());

		if(SUBSCRIBE_ALL_TOPICS)
			for (int i = 0; i < NUMBER_OF_THREADS ; i++) {
				if(GRAPH)
					executor.submit(new SimpleConsumer(topic_name,data));
				else
					executor.submit(new SimpleConsumer(topic_name,null));
			}
		else
			for (int i = 0; i < NUMBER_OF_THREADS ; i++) {
				for (int j=0; j<topic_name.size();j++)
					if(GRAPH)
						executor.submit(new SimpleConsumer(Arrays.asList(topic_name.get(j)),Arrays.asList(data.get(j))));
					else
						executor.submit(new SimpleConsumer(Arrays.asList(topic_name.get(j)),null));

			}

		SimpleProducer myProducer = new SimpleProducer(topic_name_id);
		start = 	System.currentTimeMillis();
		myProducer.produceAndPrint();
		myProducer.stop();
	}
	
	public static DefaultCategoryDataset createChart(String title) {
		
		DefaultCategoryDataset database = new DefaultCategoryDataset();
		JFrame frame = new JFrame(title);
		JPanel panel = new ChartPanel(ChartFactory.createBarChart3D(title,"","Degree (ºC)", database, 
				PlotOrientation.VERTICAL,true,true, false));
		frame.add(panel);
		frame.pack();
		frame.setVisible(true);
		return database;
	}
	
	public static List<String> readConfig(String path, boolean all) throws FileNotFoundException , IOException{
		String str;
		List <String> topics= new ArrayList<String>();
		FileReader file = new FileReader (path);
		BufferedReader buff = new BufferedReader (file);
		while((str = buff.readLine())!=null ) {
			if(all){
				String sCcaa=str.replace("#","").replace(" ", "_");
				topics.add(sCcaa);
				System.out.println(sCcaa.split(",")[0]);
			}
			else {
				if(!str.contains("#")) {
					String sCcaa=str.replace(" ", "_");
					topics.add(sCcaa);
					System.out.println(sCcaa.split(",")[0]);
				}
			}
		}
		return topics;
	}

	public static void lastRecord(String topic, double msgs, double stop) {
		// TODO Auto-generated method stub
		double time =stop-start;
		System.out.println("*********************************");
//		System.out.println(topic);
		System.out.printf("Data rate: %s records per %.3f s\n",msgs,time/1000);
		System.out.printf("Data rate: %.3f msg/s\n",msgs*1000/(time));
		System.out.println("*********************************");
	}
	
}
