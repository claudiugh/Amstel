package nl.vu.cs.amstel.msg;

import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.vu.cs.amstel.user.MessageValue;

public class OutgoingQueue {
	
	public static int LIMIT = 2;
	
	private Map<String, List<MessageValue>> queues =
		new HashMap<String, List<MessageValue>>();
	private int count = 0;
	
	private List<MessageValue> getQueue(String vertex) {
		if (!queues.containsKey(vertex)) {
			queues.put(vertex, new ArrayList<MessageValue>());
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
	
	public void add(String toVertex, MessageValue msg) {
		count++;
		getQueue(toVertex).add(msg);
	}
	
	public void sendBulk(WriteMessage w) throws IOException {
		w.writeInt(queues.keySet().size());
		for (String vertex : queues.keySet()) {
			w.writeString(vertex);
			List<MessageValue> msgs = queues.get(vertex);
			w.writeInt(msgs.size());
			for (MessageValue msg : msgs) {
				msg.serialize(w);
			}
			msgs.clear();
		}
		count = 0;
	}
}
