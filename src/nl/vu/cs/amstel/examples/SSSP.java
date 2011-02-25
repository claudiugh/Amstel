package nl.vu.cs.amstel.examples;

import nl.vu.cs.amstel.Node;
import nl.vu.cs.amstel.user.IntMessage;
import nl.vu.cs.amstel.user.IntValue;
import nl.vu.cs.amstel.user.MinIntAggregator;
import nl.vu.cs.amstel.user.MinIntCombiner;

public class SSSP {
	
	public static void main(String args[]) throws Exception {
		int nodes;
		
		if (args[0] != null) {
			nodes = Integer.parseInt(args[0]);
		} else {
			throw new Exception("Number of nodes unspecified");
		}

		Node<IntValue, IntValue, IntMessage> node =
			new Node<IntValue, IntValue, IntMessage>(nodes, SSSPVertex.class,
					IntValue.class, IntValue.class, IntMessage.class);
		node.setCombiner(MinIntCombiner.class);
		node.addAggregator(new MinIntAggregator("Destination"));
		node.run();
	}
}
