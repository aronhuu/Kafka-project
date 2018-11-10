import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UserSerializer implements Serializer<Data>{

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
	}

	@Override
	public byte[] serialize(String topic, Data data) {
		// TODO Auto-generated method stub
		if(data==null)
			return null;
		try {
			byte[] serializedName = data.getName().getBytes("UTF-8");
            int sizeOfName = serializedName.length;

            ByteBuffer buf = ByteBuffer.allocate(4+sizeOfName);
            buf.putInt(sizeOfName);
            buf.put(serializedName);


            return buf.array();
//			return new ObjectMapper().writeValueAsBytes(data);
		}catch(RuntimeException | UnsupportedEncodingException e){
			throw new SerializationException("Error serializing value",e);
		}
		
	}

}
