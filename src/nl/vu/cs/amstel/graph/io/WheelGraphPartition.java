package nl.vu.cs.amstel.graph.io;

public class WheelGraphPartition implements InputPartition {

	private static final long serialVersionUID = 3855311417929126951L;

	int vertices;
	int edges;
	int maxVertexValue;
	int maxEdgeValue;
	int workers;
	int workerIndex;
	
	public WheelGraphPartition(int workers, int workerIndex, int vertices, 
			int edges, int maxVertexValue, int maxEdgeValue) {
		this.workers = workers;
		this.workerIndex = workerIndex;
		this.vertices = vertices;
		this.edges = edges;
		this.maxVertexValue = maxVertexValue;
		this.maxEdgeValue = maxEdgeValue;
	}
}
