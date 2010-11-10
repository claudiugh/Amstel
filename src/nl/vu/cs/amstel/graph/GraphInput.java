package nl.vu.cs.amstel.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class GraphInput {
	public static int VERTEXES = 10;
	public static int EDGES = 3;
	public static String[] vertexes;
	
	static {
		init();
	}
	
	public static void init() {
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
	public static Map<String, ArrayList<String>> readVertexes(InputPartition partition) {
		HashMap<String,ArrayList<String>> data = 
			new HashMap<String, ArrayList<String>>();
		int vId = 0;
		for (int i = 0; i < vertexes.length; i++) {
			if (vertexes[i].equals(partition.from)) {
				vId = i;
				break;
			}
		}
		for (int u = vId; u < vId + partition.count; u++) {
			ArrayList<String> edges = new ArrayList<String>();
			for (int j = u + 1; j < u + EDGES + 1; j++) {
				int v = j % VERTEXES;
				edges.add(vertexes[v]);
			}
			data.put(vertexes[u], edges);
		}
		return data;
	}
}
