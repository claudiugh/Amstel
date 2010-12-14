package nl.vu.cs.amstel;

import nl.vu.cs.amstel.msg.MessageRouter;
import nl.vu.cs.amstel.user.MessageValue;

public class WorkerState<M extends MessageValue> {
	
	public int superstep;
	public int activeVertexes;
	public MessageRouter<M> router;
	public M msg;
	public boolean[] active;
	
	public WorkerState(MessageRouter<M> router) {
		this.router = router;
	}
}
