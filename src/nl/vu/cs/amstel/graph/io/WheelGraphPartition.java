package nl.vu.cs.amstel.graph.io;

public class WheelGraphPartition implements InputPartition {

	private static final long serialVersionUID = 3855311417929126951L;

	int fromVertex;
	int toVertex;
	int vertices;
	int edges;
	int maxVertexValue;
	int maxEdgeValue;
	
	public WheelGraphPartition(int fromVertex, int toVertex, int vertices, 
			int edges, int maxVertexValue, int maxEdgeValue) {
		this.fromVertex = fromVertex;
		this.toVertex = toVertex;
		this.vertices = vertices;
		this.edges = edges;
		this.maxVertexValue = maxVertexValue;
		this.maxEdgeValue = maxEdgeValue;
	}
}
