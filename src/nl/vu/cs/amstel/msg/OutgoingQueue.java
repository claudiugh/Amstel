package nl.vu.cs.amstel.msg;

import ibis.ipl.WriteMessage;

import java.io.IOException;

import nl.vu.cs.amstel.user.MessageValue;

public interface OutgoingQueue<M extends MessageValue> {

	/**
	 * buffer this message for sending
	 * @param toVertex
	 * @param msg
	 * @throws IOException
	 */
	public void add(String toVertex, M msg) throws IOException;
	
	/**
	 * flush just the filled top buffers; usually called after 
	 * reachedThreshold() returns true.
	 * @param w where we serialize the buffers
	 * @throws IOException
	 */
	public void flushFilledBuffers(WriteMessage w) throws IOException;
	
	/**
	 * Flush everything from the outgoing queue
	 * @param w
	 * @throws IOException
	 */
	public void flush(WriteMessage w) throws IOException;
	
	/**
	 * used for triggering flush operations on the queue
	 * @return true if the queue has reached a level of load where it would
	 * make sense to flush the messages to some buffers and send them over
	 * the network
	 */
	public boolean reachedThreshold();
	
	/**
	 * @return true if there is no data in the queue
	 */
	public boolean isEmpty();
}
