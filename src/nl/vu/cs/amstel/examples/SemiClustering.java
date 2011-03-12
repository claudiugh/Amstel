package nl.vu.cs.amstel.examples;

import nl.vu.cs.amstel.Node;
import nl.vu.cs.amstel.graph.io.Reader;
import nl.vu.cs.amstel.graph.io.TextFileReader;
import nl.vu.cs.amstel.user.IntValue;

public class SemiClustering {

	public static void main(String[] args) throws Exception {
		int nodes;
		String filename;
		
		if (args[0] != null) {
			nodes = Integer.parseInt(args[0]);
		} else {
			throw new Exception("Number of nodes not specified");
		}
		if (args[1] != null) {
			filename = args[1];
		} else {
			throw new Exception("Input filename not specified");
		}
		
		Reader reader = new TextFileReader(filename);
		Node<SemiClusterList, IntValue, SemiClusterMessage> node =
			new Node<SemiClusterList, IntValue, SemiClusterMessage>(nodes,
					SemiClusteringVertex.class, SemiClusterList.class,
					IntValue.class, SemiClusterMessage.class,
					reader);
		node.addAggregator(new TopClusterAggregator("BestSemiClusters"));
		node.run();
	}

}
