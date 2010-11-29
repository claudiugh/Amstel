package nl.vu.cs.amstel.msg;

import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.cs.amstel.user.MessageValue;

public class OutgoingQueue<M extends MessageValue> {
	
	public static int LIMIT = 2;
	
	private Map<String, List<M>> queues =
		new HashMap<String, List<M>>();
	private int count = 0;
	
	private List<M> getQueue(String vertex) {
		if (!queues.containsKey(vertex)) {
			queues.put(vertex, new ArrayList<M>());
		}
		return queues.get(vertex);
	}
	
	public OutgoingQueue() {
	}

	public int getCount() {
		return count;
	}
	
	public boolean reachedThreshold() {
		return count == LIMIT;
	}
	
	public void add(String toVertex, M msg) {
		count++;
		getQueue(toVertex).add(msg);
	}
	
	public void sendBulk(WriteMessage w) throws IOException {
		w.writeInt(queues.keySet().size());
		for (String vertex : queues.keySet()) {
			w.writeString(vertex);
			List<M> msgs = queues.get(vertex);
			w.writeInt(msgs.size());
			for (M msg : msgs) {
				msg.serialize(w);
			}
			msgs.clear();
		}
		count = 0;
	}
}
