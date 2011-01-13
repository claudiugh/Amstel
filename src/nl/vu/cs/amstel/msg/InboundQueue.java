package nl.vu.cs.amstel.msg;

import java.io.IOException;

import nl.vu.cs.amstel.user.MessageValue;
import ibis.ipl.ReadMessage;

/**
 * Global inbox used for buffering messages before delivering in the 
 * next super-step
 * We will have two of these inboxes, with a common local inbox (for optimizing
 * memory consumption) 
 * 
 * @author claudiugh
 *
 * @param <M>
 */
public interface InboundQueue<M extends MessageValue> {

	public void deliver(ReadMessage r) throws IOException;
	
	public void deliverLocally(int index, M msg) throws IOException;
	
	public boolean hasMessages(int index);
	
	public Iterable<M> getIterator(int index);
	
	public void clear(int index);
}
