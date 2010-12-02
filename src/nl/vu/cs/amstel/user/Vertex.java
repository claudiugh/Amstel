package nl.vu.cs.amstel.user;

import java.io.IOException;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.VertexState;
import nl.vu.cs.amstel.WorkerState;
import nl.vu.cs.amstel.msg.MessageIterator;

public abstract class Vertex<M extends MessageValue> {

	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private VertexState<M> state = null;
	private WorkerState<M> workerState = null;
	
	public void setState(VertexState<M> state) {
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
	
	public int getValue() {
		return state.getValue();
	}
	
	public void setValue(int value) {
		state.setValue(value);
	}
	
	public void voteToHalt() {
		state.setActive(false);
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
	
	abstract public void compute(MessageIterator<M> messages);
	
	public String toString() {
		return getID() + "(" + getValue() + ")";
	}

}
