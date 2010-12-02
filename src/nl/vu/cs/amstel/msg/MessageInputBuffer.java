package nl.vu.cs.amstel.msg;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import nl.vu.cs.amstel.user.MessageValue;

public class MessageInputBuffer<M extends MessageValue>
		extends ByteArrayInputStream {

	private DataInputStream dataStream;
	
	public MessageInputBuffer(byte[] buf) {
		super(buf);
		dataStream = new DataInputStream(this);
	}

	public void read(M msg) throws IOException {
		msg.deserialize(dataStream);
	}
	
	public boolean isEmpty() {
		return pos == count;
	}
}
