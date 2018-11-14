import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UserSerializer implements Serializer<Weather>{

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
	}

	@Override
	public byte[] serialize(String topic, Weather data) {
		// TODO Auto-generated method stub
		if(data==null)
			return null;
		try {
			//Serialize ccaa
			byte[] serializedCcaa = data.getCcaa().getBytes("UTF-8");
            int sizeOfCcaa = serializedCcaa.length;
            //Serialize province
            byte[] serializedProvince = data.getProvince().getBytes("UTF-8");
            int sizeOfProvince = serializedProvince.length;
            //Serialize city
			byte[] serializedCity = data.getCity().getBytes("UTF-8");
            int sizeOfCity = serializedCity.length;
            //Get the temperature data
            double maxTemp = data.getMax();
            double minTemp = data.getMin();
            
            //Put the serialize bytes in a byte buffer
            ByteBuffer buf = ByteBuffer.allocate(4+sizeOfCcaa+4+sizeOfProvince+4+sizeOfCity+8+8);
            buf.putInt(sizeOfCcaa);
            buf.put(serializedCcaa);
            buf.putInt(sizeOfProvince);
            buf.put(serializedProvince);
            buf.putInt(sizeOfCity);
            buf.put(serializedCity);
            buf.putDouble(maxTemp);
            buf.putDouble(minTemp);

            return buf.array();
//			return new ObjectMapper().writeValueAsBytes(data);
		}catch(RuntimeException | UnsupportedEncodingException e){
			throw new SerializationException("Error serializing value",e);
		}
		
	}

}
