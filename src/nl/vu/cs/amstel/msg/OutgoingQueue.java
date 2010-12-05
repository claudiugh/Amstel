package nl.vu.cs.amstel.msg;

import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.user.MessageValue;

public class OutgoingQueue<M extends MessageValue> {
	
	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	public static int PACKET_SIZE = 512; // in bytes
	private static int BUFFER_SIZE = 128; // in bytes
	
	private Map<String, MessageOutputBuffer<M>> buffers =
		new HashMap<String, MessageOutputBuffer<M>>();
	private int nonemptyBuffers = 0;
	private int estSize = 0; // estimated size, in bytes
	
	private MessageOutputBuffer<M> getBuffer(String vertex) {
		if (!buffers.containsKey(vertex)) {
			buffers.put(vertex, new MessageOutputBuffer<M>(BUFFER_SIZE));
		}
		return buffers.get(vertex);
	}
	
	public OutgoingQueue() {
	}

	public void add(String toVertex, M msg) throws IOException {
		MessageOutputBuffer<M> buffer = getBuffer(toVertex);
		int initialBytes = buffer.size();
		if (initialBytes == 0) {
			nonemptyBuffers++;
			estSize += toVertex.length();
		}
		getBuffer(toVertex).write(msg);
		estSize += buffer.size() - initialBytes;
	}
	
	public void sendBulk(WriteMessage w) throws IOException {
		/*
		logger.info("Sending a packet with " + nonemptyBuffers + " buffers and "
				+ estSize + " bytes");
		*/ 
		w.writeInt(nonemptyBuffers);
		for (String vertex : buffers.keySet()) {
			MessageOutputBuffer<M> buffer = buffers.get(vertex);
			if (buffer.size() > 0) {
				w.writeString(vertex);
				w.writeInt(buffer.size());
				w.writeArray(buffer.getBuffer(), 0, buffer.size());
				buffer.reset();
			}
		}
		nonemptyBuffers = 0;
		estSize = 0;
	}
	
	public boolean reachedThreshold() {
		return PACKET_SIZE - estSize < 10;
	}
	
	public boolean isEmpty() {
		return nonemptyBuffers == 0;
	}
}
