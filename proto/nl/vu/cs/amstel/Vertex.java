package nl.vu.cs.amstel;

import java.util.List;

abstract public class Vertex<M> {

	private VertexState state = null;
	
	public Vertex() {
	}
	
	public Vertex(VertexState state) {
		setState(state);
	}
	
	public final void setState(VertexState state) {
		this.state = state;
	}
	
	public final int getValue() {
		return state.value;
	}
	
	public final void setValue(int value) {
		state.value = value;
	}
	
	public final int getSuperstep() {
		return state.getSuperstep();
	}
	
	public final List<Integer> getOutEdges() {
		return state.getEdges();
	}
	
	public final int getId() {
		return state.getId();
	}
	
	public final void send(int dst, M msg) {
		state.getMessageRouter().send(dst, 
				new Message<M>(getSuperstep(), dst, msg)
		);
	}
	
	public final void voteToHalt() {
		state.setActive(false);
	}

	/**
	 * User should implement this
	 * @param messages
	 */
	abstract public void compute(List<Message<M>> messages);

	/**
	 * This is just for debugging
	 * @param text
	 */
	protected void log(String text) {
		System.out.println("[" + getId() + "]" + text);
	}
	
}

class VertexState {

	private int vid;
	private boolean active = true;
	public int value;
	private List<Integer> edges;

	private Worker worker;
	
	public VertexState(int vid, Worker worker, int value, List<Integer> edges) {
		this.vid = vid;
		this.worker = worker;
		this.value = value;
		this.edges = edges;
	}
	
	public List<Integer> getEdges() {
		return edges;
	}
	
	public void setActive(boolean active) {
		if (this.active && !active) {
			active = false;
			worker.activeVertexes.remove(vid);
		} else if (!this.active && active) {
			active = true;
			worker.activeVertexes.add(vid);
		}
	}
	
	public int getId() {
		return vid;
	}
	
	public int getSuperstep() {
		return worker.getSuperstep();
	}
	
	public boolean isActive() {
		return active;
	}
	
	public MessageRouter getMessageRouter() {
		return worker.msgRouter;
	}
	
	public String toString() {
		return "[" + active + "] " + value; 
	}
}
