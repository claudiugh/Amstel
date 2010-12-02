package nl.vu.cs.amstel.msg;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import nl.vu.cs.amstel.user.MessageValue;

public class MessageOutputBuffer<M extends MessageValue> 
		extends ByteArrayOutputStream {

	private DataOutputStream dataStream = new DataOutputStream(this);
	
	public MessageOutputBuffer(int size) {
		super(size);
	}
	
	public byte[] getBuffer() {
		return buf;
	}
	
	public void write(M msg) throws IOException {
		msg.serialize(dataStream);
	}
	
	public int bytesWritten() {
		return count;
	}
}
