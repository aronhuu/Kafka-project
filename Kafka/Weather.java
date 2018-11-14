
public class Weather {
	private String ccaa;
	private String province;
	private String city; 
	private double max;
	private double min;
	
	public Weather(String ccaa, String province, String city, double max, double min){
		this.ccaa=ccaa;
		this.province=province;
		this.city=city;
		this.max=max;
		this.min=min;
	}
	
	public String getCcaa() {
		return ccaa;
	}
	
	public String getProvince() {
		return province;
	}
	
	public String getCity() {
		return city;
	}
	
	public double getMax() {
		return max;
	}
	
	public double getMin() {
		return min;
	}
	
	public String toString() {
		return ccaa+" : "+province+" : "+city+ " : "+ max +"-"+min+"\n";
	}

}
