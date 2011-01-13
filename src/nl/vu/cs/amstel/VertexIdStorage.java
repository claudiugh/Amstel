package nl.vu.cs.amstel;

import java.util.HashMap;

/**
 * Class that maintains a set of vertex identifiers in memory so that we don't
 * store the same String in multiple objects.
 * By using this class the memory footprint for keeping vertex states in memory
 * decreases considerable because most of the memory is for the edges.
 * @author claudiugh
 *
 */
public class VertexIdStorage {

	private static HashMap<String, String> vertexIds =
		new HashMap<String, String>();
	
	/**
	 * Fetch the pointer of the String that stores vid.
	 * This method will be called both from the Receiver and main thread during 
	 * the Input phase, hence the concurrent access protection on vertexIds. 
	 * @param vid
	 * @return If vid is not stored, the returned String will be exactly vid and
	 * it will be added to the Storage.
	 * 		If there's another String stored with the same value, the latter
	 * will be returned.
	 */
	public static String get(String vid) {
		synchronized (vertexIds) {
			if (!vertexIds.containsKey(vid)) {
				vertexIds.put(vid, vid);
			}
		}
		return vertexIds.get(vid);
	}
}
