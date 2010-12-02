package nl.vu.cs.amstel.msg;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import nl.vu.cs.amstel.user.MessageValue;

public class MessageIterator<M extends MessageValue> implements Iterator<M>,
		Iterable<M> {

	List<byte[]> buffers = null;
	private int crtBuffer = 0;
	MessageInputBuffer<M> msgBuffer;
	private M msg;
	
	private void setCurrentBuffer(int index) {
		crtBuffer = index;
		if (buffers.size() > 0) {
			msgBuffer = new MessageInputBuffer<M>(buffers.get(index));
		}
	}
	
	public MessageIterator(M msg) {
		this.msg = msg;
	}
	
	public void setBuffers(List<byte[]> buffers) {
		this.buffers = buffers;
		setCurrentBuffer(0);
	}
	
	@Override
	public boolean hasNext() {
		if (crtBuffer >= buffers.size()) {
			return false;
		}
		return crtBuffer < buffers.size() - 1 || !msgBuffer.isEmpty();
	}

	@Override
	public M next() {
		if (!hasNext()) {
			return null;
		}
		if (msgBuffer.isEmpty()) {
			setCurrentBuffer(crtBuffer + 1);
		}
		try {
			msgBuffer.read(msg);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return msg;
	}

	@Override
	public void remove() {
		// not implemented 
	}

	@Override
	public Iterator<M> iterator() {
		return this;
	}

}
