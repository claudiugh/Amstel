package nl.vu.cs.amstel.examples;

import nl.vu.cs.amstel.Node;
import nl.vu.cs.amstel.graph.io.Reader;
import nl.vu.cs.amstel.graph.io.TextFileReader;
import nl.vu.cs.amstel.user.IntMessage;
import nl.vu.cs.amstel.user.IntValue;
import nl.vu.cs.amstel.user.MaxIntAggregator;
import nl.vu.cs.amstel.user.MaxIntCombiner;
import nl.vu.cs.amstel.user.NullValue;

public class MaxvalRunner {
	
	/**
	 * 
	 * @param args
	 *        first argument: number of nodes
	 *        second argument (optional): number of vertexes
	 * @throws Exception 
	 */
	public static void main(String args[]) throws Exception {
		int nodes;
		String filename;
		
		if (args[0] != null) {
			nodes = Integer.parseInt(args[0]);
		} else {
			throw new Exception("Number of nodes unspecified");
		}
		if (args[1] != null) {
			filename = args[1];
		} else {
			throw new Exception("Input filename not specified");
		}
		
		Reader reader = new TextFileReader(filename);
		Node<IntValue, NullValue, IntMessage> node = 
			new Node<IntValue, NullValue, IntMessage>(nodes, MaxvalVertex.class, 
					IntValue.class, NullValue.class, IntMessage.class, reader);
		node.setCombiner(MaxIntCombiner.class);
		node.addAggregator(new MaxIntAggregator("MaxVertex"));
		node.run();
	}

}
