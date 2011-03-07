package nl.vu.cs.amstel.user;

import java.io.IOException;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.AggregatorState;
import nl.vu.cs.amstel.WorkerState;
import nl.vu.cs.amstel.graph.OutEdgeIterator;
import nl.vu.cs.amstel.graph.VertexState;

public abstract class Vertex<V extends Value, E extends Value,
		M extends MessageValue> {

	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private VertexState<V, E> state = null;
	private WorkerState<E, M> workerState = null;
	
	public void setState(VertexState<V, E> state) {
		this.state = state;
	}
	
	public void setWorkerState(WorkerState<E, M> workerState) {
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
	
	public OutEdgeIterator<E> getOutEdgeIterator() {
		return workerState.edgeIterator;
	}
	
	public int getOutdegree() {
		return state.getOutdegree();
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
		OutEdgeIterator<E> iter;
		for (iter = getOutEdgeIterator(); iter.hasNext(); iter.next()) {
			String vertex = iter.getEdgeTarget();
			send(vertex, m);
		}
	}
	
	/**
	 * Output value for aggregating. Result will be available in the next
	 * super-step.
	 * 
	 * @param key
	 * @param value
	 */
	@SuppressWarnings("unchecked")
	public void outputAggregate(String key, Value value) {
		Aggregator aggregator = workerState.aggregators.get(key).aggregator;
		if (aggregator.hasValue()) {
			aggregator.combine(value);
		} else {
			aggregator.init(value);
		}
	}
	
	/**
	 * read the aggregated value from the previous super-step, for the 
	 * aggregator identified by the given key.
	 * 
	 * @param key
	 * @return
	 */
	public Value readAggregate(String key) {
		AggregatorState aggregatorState = workerState.aggregators.get(key);
		return aggregatorState.currentValue;
	}
	
	abstract public void compute(Iterable<M> messages);
	
	public String toString() {
		return getID() + "(" + getValue() + ")";
	}

}
