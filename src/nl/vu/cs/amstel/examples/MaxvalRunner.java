package nl.vu.cs.amstel.examples;

import nl.vu.cs.amstel.Node;
import nl.vu.cs.amstel.graph.GraphInput;
import nl.vu.cs.amstel.user.IntMessage;
import nl.vu.cs.amstel.user.IntValue;
import nl.vu.cs.amstel.user.MaxIntCombiner;

public class MaxvalRunner {
	
	/**
	 * 
	 * @param args
	 *        first argument: number of nodes
	 *        second argument (optional): number of vertexes
	 * @throws Exception 
	 */
	public static void main(String args[]) throws Exception {
		int nodes, vertexes, edges;
		
		if (args[0] != null) {
			nodes = Integer.parseInt(args[0]);
		} else {
			throw new Exception("Number of nodes unspecified");
		}
		if (args[1] != null) {
			vertexes = Integer.parseInt(args[1]);
			if (args[2] != null) {
				edges = Integer.parseInt(args[2]);
			} else {
				edges = vertexes / 5;
			}
			GraphInput.init(vertexes, edges);
		}
		
		Node<IntValue, IntValue, IntMessage> node = 
			new Node<IntValue, IntValue, IntMessage>(nodes, MaxvalVertex.class, 
					IntValue.class, IntValue.class, IntMessage.class);
		node.setCombiner(MaxIntCombiner.class);
		node.run();
	}

}
