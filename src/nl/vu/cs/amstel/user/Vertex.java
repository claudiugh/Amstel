package nl.vu.cs.amstel.user;

import java.io.IOException;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.Value;
import nl.vu.cs.amstel.VertexState;
import nl.vu.cs.amstel.WorkerState;

public abstract class Vertex<V extends Value, M extends MessageValue> {

	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private VertexState<V, M> state = null;
	private WorkerState<M> workerState = null;
	
	public void setState(VertexState<V, M> state) {
		this.state = state;
	}
	
	public void setWorkerState(WorkerState<M> workerState) {
		this.workerState = workerState;
	}
	
	public String getID() {
		return state.getID();
	}
	
	public int getSuperstep() {
		return workerState.superstep;
	}
	
	public M newMessage() {
		return workerState.msg;
	}
	
	public V getValue() {
		return state.getValue();
	}
	
	public void voteToHalt() {
		workerState.active[state.getIndex()] = false;
	}
	
	public String[] getOutEdges() {
		return state.getOutEdges();
	}
	
	public void send(String toVertex, M m) {
		try {
			workerState.router.send(toVertex, m);
		} catch (IOException e) {
			logger.error("Error sending message " + m + " to " + toVertex);
			e.printStackTrace();
		}
	}
	
	public void sendToAll(M m) {
		for (String v : getOutEdges()) {
			send(v, m);
		}
	}
	
	abstract public void compute(Iterable<M> messages);
	
	public String toString() {
		return getID() + "(" + getValue() + ")";
	}

}
