package nl.vu.cs.amstel.msg;

import ibis.ipl.WriteMessage;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.user.Combiner;
import nl.vu.cs.amstel.user.MessageValue;

public class CombinedOutgoingQueue<M extends MessageValue> 
		implements OutgoingQueue<M> {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private static final int OUTPUT_BUFFER_SIZE = 4096;
	
	protected MessageFactory<M> msgFactory;
	protected Combiner<M> combiner;
	protected Map<String, M> buffers = new HashMap<String, M>();

	private int nonemptyBuffers = 0; 
	private ByteArrayOutputStream sendBuffer = 
		new ByteArrayOutputStream(OUTPUT_BUFFER_SIZE);	
	private DataOutputStream outStream = new DataOutputStream(sendBuffer);
	
	public CombinedOutgoingQueue(MessageFactory<M> msgFactory) {
		this.msgFactory = msgFactory;
		this.combiner = msgFactory.createCombiner();
	}
	
	@Override
	public void add(String toVertex, M msg) throws IOException {
		if (!buffers.containsKey(toVertex)) {
			M buffer = msgFactory.create();
			buffer.copy(msg);
			buffers.put(toVertex, buffer);
			nonemptyBuffers++;
		} else {
			// combine the two existing values (the buffered and the new one)
			// and replace the buffered value with the combined one
			M buffer = buffers.get(toVertex);
			M combined = combiner.combine(buffer, msg);
			buffer.copy(combined);
		}
	}

	@Override
	public void flush(WriteMessage w) throws IOException {
		w.writeInt(buffers.size());
		for (String dst : buffers.keySet()) {
			M msg = buffers.get(dst);
			outStream.writeUTF(dst);
			msg.serialize(outStream);
		}
		// remove all messages
		buffers.clear();
		nonemptyBuffers = 0;
		// this could be optimized by not copying the buffer
		w.writeInt(sendBuffer.size());
		w.writeArray(sendBuffer.toByteArray());
		sendBuffer.reset();
	}

	@Override
	public void flushFilledBuffers(WriteMessage w) throws IOException {
		// not called for now
	}

	@Override
	public boolean isEmpty() {
		return nonemptyBuffers == 0;
	}

	@Override
	public boolean reachedThreshold() {
		return false;
	}

}
