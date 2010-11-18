package nl.vu.cs.amstel.user;

import java.io.IOException;
import java.util.List;

import nl.vu.cs.amstel.VertexState;
import nl.vu.cs.amstel.WorkerState;

public class Vertex {

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
	
	public void send(String toVertex, MessageValue m) throws IOException {
		workerState.router.send(toVertex, m);
	}
	
	private void sendToAll(MessageValue m) throws IOException {
		for (String v : getOutEdges()) {
			send(v, m);
		}
	}
	
	public void compute(List<MessageValue> messages) throws IOException {
		System.out.println("Running compute for " + getID());
		if (getSuperstep() == 0) {
			sendToAll(new MessageValue(getValue()));
			return;
		}
		int max = getValue();
		for (MessageValue m : messages) {
			if (m.value > max) {
				max = m.value;
			}
		}
		if (max != getValue()) {
			setValue(max);
			sendToAll(new MessageValue(getValue()));
		} else {
			voteToHalt();
		}
	}
}
