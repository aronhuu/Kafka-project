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
	
	final static int NUMBER_OF_THREADS = 1;
	static double start;
	public static void main(String[] args) {
		
		List <String> topic_ccaas = null;
		try {
			 topic_ccaas = readConfig("topic.conf");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		//The topics don't allow spaces between words, underscore needed
//		String [] topic_ccaas = {"CANARIAS"};//,"COMUNIDAD_DE_MADRID","GALICIA"};//,"COMUNIDAD_DE_MADRID"};//CANARIAS, EXTREMADURA
		DefaultCategoryDataset [] data= new DefaultCategoryDataset [topic_ccaas.size()];
		for (int i=0; i<topic_ccaas.size();i++) {
			data[i]  = createChart("Weather in "+topic_ccaas.get(i));//,createChart("Weather in Madrid")
		}
				//,createChart("Weather in Galicia")};

		for (String topic : topic_ccaas){
			ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
	
			for (int i = 0; i < NUMBER_OF_THREADS ; i++) {
				executor.submit(new SimpleConsumer(topic_ccaas,data));
			}
		}
		
//		//subscribe to various topics
//		ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
//		executor.submit(new SimpleConsumer(topic_ccaas,data));
//		executor.submit(new SimpleConsumer(topic_ccaas,data));

		SimpleProducer myProducer = new SimpleProducer(topic_ccaas);
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
//		frame.setLocationRelativeTo(null);
		frame.setVisible(true);
		return database;
	}
	
	public static List<String> readConfig(String path) throws FileNotFoundException , IOException{
		String str;
		List <String> topics= new ArrayList<String>();
		FileReader file = new FileReader (path);
		BufferedReader buff = new BufferedReader (file);
		while((str = buff.readLine())!=null ) {
			if(!str.contains("#")){
				topics.add(str.replace(" ", "_"));
				System.out.println(str);
			}
		}
		return topics;
	}

	public static void lastRecord(String topic, double msgs, double stop) {
		// TODO Auto-generated method stub
		double time =stop-start;
		System.out.println("*********************************");
		System.out.println(topic);
		System.out.printf("Data rate: %s records per %.3f s\n",msgs,time/1000);
		System.out.printf("Data rate: %.3f msg/s\n",msgs*1000/(time));
		System.out.println("*********************************");
	}
	
}
