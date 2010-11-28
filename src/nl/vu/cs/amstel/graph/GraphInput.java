package nl.vu.cs.amstel.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class GraphInput {
	
	public static Random rand = new Random();
	
	public static int VERTEXES = 20;
	public static int EDGES = 3;
	public static String[] vertexes;
	
	public static void init(int vertexesNo, int edges) {
		VERTEXES = vertexesNo;
		EDGES = edges;
		vertexes = new String[VERTEXES];
		for (int i = 0; i < VERTEXES; i++) {
			vertexes[i] = "V" + i;
		}
	}
	
	/**
	 * generates the edges for a partition of the input
	 * @param partition
	 * @return
	 */
	public static Map<String, String[]> readVertexes(
			InputPartition partition) {
		HashMap<String, String[]> data = 
			new HashMap<String, String[]>();
		int vId = 0;
		for (int i = 0; i < vertexes.length; i++) {
			if (vertexes[i].equals(partition.from)) {
				vId = i;
				break;
			}
		}
		for (int u = vId; u < vId + partition.count; u++) {
			String[] edges = new String[EDGES];
			for (int j = 0; j < EDGES; j++) {
				int v = (j + u + 1) % VERTEXES;
				edges[j] = vertexes[v];
			}
			data.put(vertexes[u], edges);
		}
		return data;
	}
	
	public static int readValue(String vertex) {
		return rand.nextInt(VERTEXES);
	}
}
