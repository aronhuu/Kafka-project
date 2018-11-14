import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class UserDeserializer implements Deserializer <Weather> {

	private boolean isKey;

	public UserDeserializer() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
	}


	@Override
	public Weather deserialize(String s, byte[] value) {
		// TODO Auto-generated method stub
		if(value==null) {
			System.out.println("Null recieved at deserialize");
			return null;
		}
		try {
			//Read the bytes from the buffer	
			ByteBuffer buf = ByteBuffer.wrap(value);
			
			//Deserialize the autonomous community
			int sizeOfCcaa = buf.getInt();
			byte[] ccaaBytes = new byte[sizeOfCcaa];
            buf.get(ccaaBytes);         
            String ccaa = new String(ccaaBytes, "UTF-8");
			//Deserialize the province
			int sizeOfProvince = buf.getInt();
			byte[] provinceBytes = new byte[sizeOfProvince];
            buf.get(provinceBytes);         
            String province = new String(provinceBytes, "UTF-8");
			//Deserialize the city
			int sizeOfCity = buf.getInt();
			byte[] cityBytes = new byte[sizeOfCity];
            buf.get(cityBytes);         
            String city = new String(cityBytes, "UTF-8");
			//Deserialize the temperature data
            double max = buf.getDouble();
            double min = buf.getDouble();
                      
			return new Weather(ccaa, province, city, max, min);
//            return new ObjectMapper().readValue(value, Data.class);
		}catch(RuntimeException | IOException e){
			throw new SerializationException("Error serializing value",e);
		}
	}

}
