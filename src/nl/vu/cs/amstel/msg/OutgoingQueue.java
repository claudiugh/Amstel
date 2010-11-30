package nl.vu.cs.amstel.msg;

import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.cs.amstel.user.MessageValue;

public class OutgoingQueue<M extends MessageValue> {
	
	public static int QUEUE_SIZE = 64;
	
	private Map<String, List<M>> queues =
		new HashMap<String, List<M>>();
	private int count = 0;
	private int nonemptyQueues = 0;
	
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
		return count == QUEUE_SIZE;
	}
	
	public void add(String toVertex, M msg) {
		List<M> queue = getQueue(toVertex);
		if (queue.size() == 0) {
			nonemptyQueues++;
		}
		count++;
		getQueue(toVertex).add(msg);
	}
	
	public void sendBulk(WriteMessage w) throws IOException {
		w.writeInt(nonemptyQueues);
		for (String vertex : queues.keySet()) {
			List<M> msgs = queues.get(vertex);
			if (msgs.size() > 0) {
				w.writeString(vertex);
				w.writeInt(msgs.size());
				for (M msg : msgs) {
					msg.serialize(w);
				}
				msgs.clear();
			}
		}
		count = 0;
		nonemptyQueues = 0;
	}
}
