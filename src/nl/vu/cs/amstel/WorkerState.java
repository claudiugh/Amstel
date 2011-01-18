package nl.vu.cs.amstel;

import nl.vu.cs.amstel.graph.ArrayOutEdgeIterator;
import nl.vu.cs.amstel.msg.MessageRouter;
import nl.vu.cs.amstel.user.MessageValue;
import nl.vu.cs.amstel.user.Value;

public class WorkerState<E extends Value, M extends MessageValue> {
	
	public int superstep;
	public int activeVertexes;
	public MessageRouter<?, E, M> router;
	public ArrayOutEdgeIterator<E> edgeIterator;
	public M msg;
	public boolean[] active;
	
	public WorkerState(MessageRouter<?, E, M> router) {
		this.router = router;
	}
}
