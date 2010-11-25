package nl.vu.cs.amstel.user;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.VertexState;
import nl.vu.cs.amstel.WorkerState;

public abstract class Vertex {

	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private VertexState state;
	private WorkerState workerState;
	
	public void setState(VertexState state) {
		this.state = state;
	}
	
	public void setWorkerState(WorkerState workerState) {
		this.workerState = workerState;
	}
	
	public String getID() {
		return state.getID();
	}
	
	public int getSuperstep() {
		return workerState.superstep;
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
	
	public List<String> getOutEdges() {
		return state.getOutEdges();
	}
	
	public void send(String toVertex, MessageValue m) {
		try {
			workerState.router.send(toVertex, m);
		} catch (IOException e) {
			logger.error("Error sending message " + m + " to " + toVertex);
			e.printStackTrace();
		}
	}
	
	public void sendToAll(MessageValue m) {
		for (String v : getOutEdges()) {
			logger.info("Sending " + m + " to " + v);
			send(v, m);
		}
	}
	
	abstract public void compute(List<MessageValue> messages);
	
	public String toString() {
		return getID() + "(" + getValue() + ")";
	}
	
}
