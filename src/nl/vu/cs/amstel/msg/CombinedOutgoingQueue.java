package nl.vu.cs.amstel.msg;

import ibis.ipl.WriteMessage;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.user.Combiner;
import nl.vu.cs.amstel.user.MessageValue;

public class CombinedOutgoingQueue<M extends MessageValue> 
		implements OutgoingQueue<M> {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private static final int OUTPUT_BUFFER_SIZE = 4096;
	private static final int COUNT_THRESHOLD = 1024;
	
	protected MessageFactory<M> msgFactory;
	protected Combiner<M> combiner;
	
	private Map<String, M> buffers = new HashMap<String, M>();
	private LinkedList<String> filled = new LinkedList<String>();

	private ByteArrayOutputStream sendBuffer = 
		new ByteArrayOutputStream(OUTPUT_BUFFER_SIZE);	
	private DataOutputStream outStream = new DataOutputStream(sendBuffer);
	
	public CombinedOutgoingQueue(MessageFactory<M> msgFactory) {
		this.msgFactory = msgFactory;
		this.combiner = msgFactory.createCombiner();
	}
	
	@Override
	public void add(String toVertex, M msg) throws IOException {
		if (!buffers.containsKey(toVertex) || buffers.get(toVertex) == null) {
			M buffer = msgFactory.create();
			buffer.copy(msg);
			buffers.put(toVertex, buffer);
			filled.addLast(toVertex);
		} else {
			// combine the two existing values (the buffered and the new one)
			// and replace the buffered value with the combined one
			M buffer = buffers.get(toVertex);
			M combined = combiner.combine(buffer, msg);
			buffer.copy(combined);
		}
	}

	protected void sendBulk(WriteMessage w, int count) throws IOException {
		w.writeInt(count);
		for (int i = 0; i < count; i++) {
			String dst = filled.removeFirst();
			M msg = buffers.get(dst);
			outStream.writeUTF(dst);
			msg.serialize(outStream);
			buffers.put(dst, null);
		}
		// this could be optimized by not copying the buffer
		w.writeInt(sendBuffer.size());
		w.writeArray(sendBuffer.toByteArray());
		sendBuffer.reset();		
	}
	
	@Override
	public void flush(WriteMessage w) throws IOException {
		sendBulk(w, filled.size());
	}

	@Override
	public void flushFilledBuffers(WriteMessage w) throws IOException {
		sendBulk(w, COUNT_THRESHOLD / 2);
	}

	@Override
	public boolean isEmpty() {
		return filled.size() == 0;
	}

	@Override
	public boolean reachedThreshold() {
		return filled.size() > COUNT_THRESHOLD;
	}

}
