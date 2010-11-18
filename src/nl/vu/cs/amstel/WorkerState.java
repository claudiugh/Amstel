package nl.vu.cs.amstel;

import nl.vu.cs.amstel.msg.MessageRouter;

public class WorkerState {
	
	public int superstep;
	public int activeVertexes;
	public MessageRouter router;
	
	public WorkerState(MessageRouter router) {
		this.router = router;
	}
}
