import java.io.File;
import java.io.IOException;
import java.util.Properties;

import javax.swing.JPanel;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYBarRenderer;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.data.xy.IntervalXYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class Histogram extends ApplicationFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	final static int NUM_CITIES = 200;

	Properties props;
	static String [] cities = new String[NUM_CITIES];
	static double[] tempMax = new double[NUM_CITIES];
	static double [] tempMin = new double[NUM_CITIES];
	
	public Histogram(String title) {
		super(title);
		JPanel chartPanel = crearPanel();
		chartPanel.setPreferredSize(new java.awt.Dimension(500, 475));
		setContentPane(chartPanel);
	}
	
	private static IntervalXYDataset crearDataset() {
		HistogramDataset dataset = new HistogramDataset();
		//vecto almacena los ingresos quincenales de 45 personas
//		double vector[] = {63, 89, 36, 49, 56, 64, 59, 35, 78,
//							43, 53, 70, 57, 62, 43, 68, 62, 26,
//							64, 72, 52, 51, 62, 60, 71, 61, 55,
//							59, 60, 67, 57, 67, 61, 67, 51, 81,
//							53, 64, 76, 44, 73, 56, 62, 63, 60};
		//En el ejercicio nos piden construir una distribución de frecuencias de 8 intervalos
		//Por eso ponemos 8 en el tercer parámetro del addSeries
		//dataset.addSeries("Temperatura Máxima", tempMax, 8);
		dataset.addSeries("Temperatura Mínima", tempMin, 10);
		return dataset;
	}
	private static JFreeChart crearChart(IntervalXYDataset dataset) {
		JFreeChart chart = ChartFactory.createHistogram(
							"Histograma",
							null,
							null,
							dataset,
							PlotOrientation.VERTICAL,
							true,
							true,
							false
							);
		XYPlot plot = (XYPlot) chart.getPlot();
		XYBarRenderer renderer = (XYBarRenderer) plot.getRenderer();
		renderer.setDrawBarOutline(false);
//		try{
//			ChartUtilities.saveChartAsJPEG(new File("C:\\histograma.jpg"), chart, 500, 475);
//		}
//		catch(IOException e){
//			System.out.println("Error al abrir el archivo");
//		}
		return chart;
	}
	
	public static JPanel crearPanel() {
		JFreeChart chart = crearChart(crearDataset());
		return new ChartPanel(chart);
	}
	
	static void parser() {
		int counter=0;
		try {
	         File inputFile = new File("20181109_t_prev_esp.xml");
	         DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
	         DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
	         Document doc = dBuilder.parse(inputFile);
	         doc.getDocumentElement().normalize();
	         //System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
	         NodeList ccaaList = doc.getElementsByTagName("ccaa");
	         //System.out.println("----------------------------");
	         
	         for (int i = 0; i < ccaaList.getLength(); i++) {
	        	 Element ccaa = (Element) ccaaList.item(i);
	        	 //System.out.println("\nCOMUNIDAD AUT�NOMA: " + ccaa.getAttribute("nombre"));
	        	 
	            	//Entrar a las provincias
	                NodeList provinicasList = ccaa.getElementsByTagName("provincia");
	                
	                for (int j = 0; j < provinicasList.getLength(); j++) {
	                	Element provincia = (Element) provinicasList.item(j);
	                	//System.out.println("\n*PROVINCIA: " + provincia.getAttribute("nombre"));
	                	
	                	//Entrar a ciudades
	                	 NodeList ciudadesList = provincia.getElementsByTagName("ciudad");
	                	
	                	 for(int k = 0; k < ciudadesList.getLength() ; k++) {
	                		Element ciudad = (Element) ciudadesList.item(k);
	                		//System.out.println("\n>>>CIUDAD: " + ciudad.getAttribute("nombre"));
	                		cities[counter] = ciudad.getAttribute("nombre");
	                		
	                		
	                		//Recover tmax
	                		NodeList tmaxList = ciudad.getElementsByTagName("tmax");
	                		Element tmax = (Element) tmaxList.item(0); // There is just one description item
	                		tempMax[counter] = Double.parseDouble(tmax.getTextContent().trim());
	                		//System.out.println("\n-------TMAX: " + tmax.getTextContent().trim());
	                		
	                		
	                		//Recover tmin
	                		NodeList tminList = ciudad.getElementsByTagName("tmin");
	                		Element tmin = (Element) tminList.item(0); // There is just one description item
	                		//System.out.println("\n-------TMIN: " + tmin.getTextContent().trim());
	                		tempMin[counter] = Double.parseDouble(tmin.getTextContent().trim());
	                		counter++;
	                	 }
	                }
	            }
	      } catch (Exception e) {
	         e.printStackTrace();
	      }

	}
	
	
	public static void main(String[] args) throws IOException {
		parser();
		
		for (int i = 0; i < cities.length; i++) {
			// Fire-and-forget send(topic, key, value)
			// Send adds records to unsent records buffer and return
			System.out.println(cities[i]);
		//("TEMPERATURES", "temperature-min", cities[i] + "_" +tempMin[i] ));
			}
		
//
		Histogram histo = new Histogram("Histograma");
		histo.pack();
		RefineryUtilities.centerFrameOnScreen(histo);
		histo.setVisible(true);
	}
}