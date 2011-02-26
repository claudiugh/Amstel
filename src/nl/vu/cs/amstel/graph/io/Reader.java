package nl.vu.cs.amstel.graph.io;

import java.io.IOException;

import nl.vu.cs.amstel.graph.VertexFactory;
import nl.vu.cs.amstel.graph.VertexState;
import nl.vu.cs.amstel.user.Value;

public interface Reader {

	InputPartitioner getPartitioner();
	
	void close() throws IOException;
	
	<V extends Value, E extends Value> VertexState<V, E> 
		nextVertex(VertexFactory<V, E> factory) throws IOException;
	
	boolean hasNext() throws IOException;
}
