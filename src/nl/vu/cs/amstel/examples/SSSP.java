package nl.vu.cs.amstel.examples;

import nl.vu.cs.amstel.Node;
import nl.vu.cs.amstel.graph.io.Reader;
import nl.vu.cs.amstel.graph.io.TextFileReader;
import nl.vu.cs.amstel.user.IntMessage;
import nl.vu.cs.amstel.user.IntValue;
import nl.vu.cs.amstel.user.MinIntAggregator;
import nl.vu.cs.amstel.user.MinIntCombiner;

public class SSSP {
	
	public static void main(String args[]) throws Exception {
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
		if (args[2] != null && args[3] != null) {
			SSSPVertex.SRC = args[2];
			SSSPVertex.DST = args[3];
		} else {
			throw new Exception("Source and destination not specified");
		}
		
		Reader reader = new TextFileReader(filename);
		Node<IntValue, IntValue, IntMessage> node =
			new Node<IntValue, IntValue, IntMessage>(nodes, SSSPVertex.class,
					IntValue.class, IntValue.class, IntMessage.class, reader);
		node.setCombiner(MinIntCombiner.class);
		node.addAggregator(new MinIntAggregator("Destination"));
		node.run();
	}
}
