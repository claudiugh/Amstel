package nl.vu.cs.amstel.msg;

import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.user.MessageValue;

public class SerializedOutgoingQueue<M extends MessageValue> 
		implements OutgoingQueue<M>{
	
	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	public static int PACKET_SIZE = 512; // bytes
	private static int BUFFER_SIZE = 128; // bytes
	private static int FILLED_BUFF_SIZE_THOLD = 120; // bytes
	private static int FILLED_BUFF_CNT_THOLD = 32;
	
	private Map<String, MessageOutputBuffer<M>> buffers =
		new HashMap<String, MessageOutputBuffer<M>>();
	
	private MessageOutputBuffer<M> head = null;
	private MessageOutputBuffer<M> tail = null;
	private int filledBuffers = 0;
	private int nonemptyBuffers = 0; // number of buffers (that have data)

	private MessageOutputBuffer<M> getBuffer(String vertex) {
		if (!buffers.containsKey(vertex)) {
			buffers.put(vertex, new MessageOutputBuffer<M>(BUFFER_SIZE, 
					vertex));
		}
		return buffers.get(vertex);
	}
	
	private MessageOutputBuffer<M> extractMaxBuffer() {
		MessageOutputBuffer<M> buffer = head;
		head = head.next;
		if (head != null) {
			head.prev = null;
		} else {
			tail = null;
		}
		nonemptyBuffers--;
		return buffer;
	}
	
	private void swap(MessageOutputBuffer<M> a, MessageOutputBuffer<M> b) {
		MessageOutputBuffer<M> tmp;
		// first swap the links
		// if they are neighbors we need a special treatment
		if (a.next == b) {
			a.next = b.next;
			b.next = a;
			b.prev = a.prev;
			a.prev = b;
		} else if (a.prev == b) {
			b.next = a.next;
			a.next = b;
			a.prev = b.prev;
			b.prev = a;
		} else {
			tmp = a.next;
			a.next = b.next;
			b.next = tmp;
			tmp = a.prev;
			a.prev = b.prev;
			b.prev = tmp;
		}
		// enforce link consistency
		// 1. for "a"
		if (a.next != null) {
			a.next.prev = a;
		} else {
			tail = a;
		}
		if (a.prev != null) {
			a.prev.next = a;
		} else {
			head = a;
		}
		// 2. for "b"
		if (b.next != null) {
			b.next.prev = b;
		} else {
			tail = b;
		}
		if (b.prev != null) {
			b.prev.next = b;
		} else {
			head = b;
		}
	}
	
	private void checkList() {
		int l1, l2;
		MessageOutputBuffer<M> b;
		for (b = head, l1 = 0; b != null && l1 < nonemptyBuffers; l1++, b = b.next) {
		}
		if (b != null) {
			logger.fatal("List is corrupt");
			System.exit(1);
		}
		for (b = tail, l2 = 0; b != null && l2 < nonemptyBuffers; l2++, b = b.prev) {
		}
		if (b != null) {
			System.exit(1);
			logger.fatal("List is corrupt");
		}
	}
	
	private void sendBuffer(WriteMessage w, MessageOutputBuffer<M> buffer) 
			throws IOException {
		w.writeString(buffer.getDestination());
		w.writeInt(buffer.size());
		w.writeArray(buffer.getBuffer(), 0, buffer.size());
		buffer.reset();
	}
	
	/**
	 * send just a few filled buffers
	 * @param w
	 * 
	 * @throws IOException
	 */
	private void sendBulk(WriteMessage w, int count) throws IOException {
		w.writeInt(count);
		for (int i = 0; i < count; i++) {
			sendBuffer(w, extractMaxBuffer());
		}
	}
	
	public void add(String toVertex, M msg) throws IOException {
		MessageOutputBuffer<M> buffer = getBuffer(toVertex);
		if (buffer.size() == 0) {
			// append the buffer to the end of list
			nonemptyBuffers++;
			buffer.prev = tail;
			if (buffer.prev != null) {
				buffer.prev.next = buffer;
			} else {
				head = buffer;
			}
			buffer.next = null;
			tail = buffer;
		}
		buffer.write(msg);
		if (buffer.size() > FILLED_BUFF_SIZE_THOLD) {
			filledBuffers++;
		}
		// we have just written data in the buffer,
		// so we have to update the priority list
		if (buffer != head) {
			MessageOutputBuffer<M> pos = buffer.prev;
			while (pos.prev != null && buffer.size() > pos.size()) {
				pos = pos.prev;
			}
			if (buffer.size() > pos.size()) {
				swap(pos, buffer);
			}
		}
	}
	
	public void flushFilledBuffers(WriteMessage w) throws IOException {
		// we know that are at least this number of filled buffers
		sendBulk(w, FILLED_BUFF_CNT_THOLD);
		filledBuffers -= FILLED_BUFF_CNT_THOLD;
	}
	
	public void flush(WriteMessage w) throws IOException {
		// send all buffers
		sendBulk(w, nonemptyBuffers);
	}
	
	public boolean reachedThreshold() {
		return filledBuffers > FILLED_BUFF_CNT_THOLD - 1;
	}
	
	public boolean isEmpty() {
		return head == null;
	}

	public SerializedOutgoingQueue() {
	}

}
