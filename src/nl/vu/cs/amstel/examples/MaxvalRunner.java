package nl.vu.cs.amstel.examples;

import nl.vu.cs.amstel.Node;
import nl.vu.cs.amstel.graph.GraphInput;
import nl.vu.cs.amstel.user.IntMessage;

public class MaxvalRunner {
	
	/**
	 * 
	 * @param args
	 *        first argument: number of nodes
	 *        second argument (optional): number of vertexes
	 * @throws Exception 
	 */
	public static void main(String args[]) throws Exception {
		int nodes, vertexes;
		
		if (args[0] != null) {
			nodes = Integer.parseInt(args[0]);
		} else {
			throw new Exception("Number of nodes unspecified");
		}
		
		if (args[1] != null) {
			vertexes = Integer.parseInt(args[1]);
			GraphInput.init(vertexes, vertexes / 4);
		}
		
		try {
			new Node<IntMessage>().run(nodes, MaxvalVertex.class, IntMessage.class);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}

}
