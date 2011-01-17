package nl.vu.cs.amstel.graph;

import java.util.Iterator;

import nl.vu.cs.amstel.user.Value;

public interface OutEdgeIterator<E extends Value> extends Iterator<String> {

	public String getEdgeTarget();
	
	public E getEdgeValue();
}
