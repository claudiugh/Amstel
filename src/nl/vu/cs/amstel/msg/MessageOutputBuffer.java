package nl.vu.cs.amstel.msg;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import nl.vu.cs.amstel.user.MessageValue;

public class MessageOutputBuffer<M extends MessageValue> 
		extends ByteArrayOutputStream 
		implements Comparable<MessageOutputBuffer<M>>{
	
	// links for the priority list
	public MessageOutputBuffer<M> prev = null;
	public MessageOutputBuffer<M> next = null;
	
	private DataOutputStream dataStream = new DataOutputStream(this);
	private String dst; // destination vertex
	
	public MessageOutputBuffer(int size, String dst) {
		super(size);
		this.dst = dst;
	}
	
	public byte[] getBuffer() {
		return buf;
	}
	
	public void write(M msg) throws IOException {
		msg.serialize(dataStream);
	}
	
	public String getDestination() {
		return dst;
	}
	
	@Override
	public int compareTo(MessageOutputBuffer<M> other) {
		return size() - other.size();
	}
	
}
