package nl.vu.cs.amstel.msg;

import java.util.Iterator;

import nl.vu.cs.amstel.user.MessageValue;

/**
 * Iterator over a single message, used for combined inbox
 * IMPORTANT: not reentrant
 * @author claudiugh
 *
 * @param <M>
 */
public class SingleMessageIterator<M extends MessageValue> 
	implements Iterator<M>, Iterable<M> {

	private M msg;
	private boolean active = false;
	private boolean visited = false;
	
	public SingleMessageIterator(M msg) {
		this.msg = msg;
	}
	
	public void enable(M msg) {
		this.msg.copy(msg);
		active = true;
	}
	
	public void disable() {
		active = false;
	}
	
	@Override
	public Iterator<M> iterator() {
		visited = false;
		return this;
	}

	@Override
	public boolean hasNext() {
		return active && !visited;
	}

	@Override
	public M next() {
		if (!hasNext()) {
			return null;
		}
		active = false;
		visited = true;
		return msg;
	}

	@Override
	public void remove() {
		// unimplemented 
	}

}
