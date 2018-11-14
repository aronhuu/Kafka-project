import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.swing.JFrame;
import javax.swing.JPanel;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;


public class kafka_system {
	
	final static int NUMBER_OF_THREADS = 2;
	public static void main(String[] args) {
		
		//The topics don't allow spaces between words, underscore needed
		String [] topic_ccaas = {"COMUNIDAD_VALENCIANA","COMUNIDAD_DE_MADRID","GALICIA"};//,"COMUNIDAD_DE_MADRID"};//CANARIAS, EXTREMADURA
		DefaultCategoryDataset []data= {createChart("Weather in Valencia"),createChart("Weather in Madrid")
				,createChart("Weather in Galicia")};

//		for (String topic : topic_ccaas){
//			ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
//	
//			for (int i = 0; i < NUMBER_OF_THREADS ; i++) {
//				executor.submit(new SimpleConsumer(topic));
//			}
//		}
		
		//subscribe to various topics
		ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
		executor.submit(new SimpleConsumer(Arrays.asList(topic_ccaas),data));
		//executor.submit(new SimpleConsumer(Arrays.asList(topic_ccaas)));

		SimpleProducer myProducer = new SimpleProducer(topic_ccaas);
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
	
}
