package nl.vu.cs.amstel.graph;

import nl.vu.cs.amstel.user.Value;

public class ArrayOutEdgeIterator<E extends Value> implements OutEdgeIterator<E> {

	private int index = -1;
	private String[] edges = null;
	private E[] values = null;
	
	public ArrayOutEdgeIterator() {
	}
	
	public void reset(String[] edges, E[] values) {
		index = 0;
		this.edges = edges;
		this.values = values;
	}
	
	public void reset() {
		index = 0;
	}
	
	@Override
	public String getEdgeTarget() {
		return edges[index];
	}

	@Override
	public E getEdgeValue() {
		return values[index];
	}

	@Override
	public boolean hasNext() {
		return index < edges.length;
	}

	@Override
	public String next() {
		String target = edges[index];
		index++;
		return target;
	}

	@Override
	public void remove() {
		// probably useful in topology mutations
	}

}
