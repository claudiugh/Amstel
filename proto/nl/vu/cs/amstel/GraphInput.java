package nl.vu.cs.amstel;

import java.util.ArrayList;

public class GraphInput {
	public static ArrayList<Integer>[] graph;
	
	static {
		init();
	}
	
	public static void init() {
		graph = new ArrayList[6];
		for (int i = 0; i < graph.length; i++) {
			graph[i] = new ArrayList<Integer>();
		}
		// the first element is the value of the vertex
		graph[0].add(3);
		graph[1].add(6);
		graph[2].add(2);
		graph[3].add(1);
		graph[4].add(4);
		graph[5].add(7);
		// edges 
		graph[0].add(1);
		graph[0].add(2);
		graph[1].add(0);
		graph[1].add(3);
		graph[2].add(1);
		graph[2].add(3);
		graph[3].add(1);
		graph[3].add(5);
		graph[3].add(4);
		graph[4].add(3);
		graph[4].add(5);
		graph[5].add(3);
	}
	
}
