import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class UserDeserializer implements Deserializer <Data> {

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
	public Data deserialize(String s, byte[] value) {
		// TODO Auto-generated method stub
		if(value==null) {
			System.out.println("Null recieved at deserialize");
			return null;
		}
		try {
			
			ByteBuffer buf = ByteBuffer.wrap(value);
			int sizeOfName = buf.getInt();
			byte[] nameBytes = new byte[sizeOfName];
            buf.get(nameBytes);
            String deserializedName = new String(nameBytes, "UTF-8");
            
			return new Data(deserializedName);
//            return new ObjectMapper().readValue(value, Data.class);
		}catch(RuntimeException | IOException e){
			throw new SerializationException("Error serializing value",e);
		}
	}

}
