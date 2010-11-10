package nl.vu.cs.amstel.graph;

import java.io.Serializable;

/**
 * represents a partition of the graph
 * @author claudiugh
 *
 */
public class InputPartition implements Serializable {
	
	private static final long serialVersionUID = 1L;

	// from index of the vertex where we start
	public String from;
	public int count;

	public InputPartition(String from, int count) {
		this.from = from;
		this.count = count;
	}
	
	public String toString() {
		return "From " + from + ", " + count + " nodes";
	}
}
