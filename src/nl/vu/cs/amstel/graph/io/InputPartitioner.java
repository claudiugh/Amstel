package nl.vu.cs.amstel.graph.io;

import java.io.IOException;

public interface InputPartitioner {
	
	InputPartition[] getPartitions(int workers) throws IOException;
}
