package nl.vu.cs.amstel.examples;

import nl.vu.cs.amstel.user.IntMessage;
import nl.vu.cs.amstel.user.IntValue;
import nl.vu.cs.amstel.user.Vertex;

public class MaxvalVertex extends Vertex<IntValue, IntMessage> {

	public void compute(Iterable<IntMessage> messages) {
		IntValue max = getValue();
		IntMessage outMsg = newMessage();
		if (getSuperstep() == 0) {
			outMsg.value = max.value;
			sendToAll(outMsg);
			return;
		}
		int localMax = max.value;
		for (IntMessage m : messages) {
			if (m.value > localMax) {
				localMax = m.value;
			}
		}
		if (max.value != localMax) {
			max.value = localMax;
			outMsg.value = localMax;
			sendToAll(outMsg);
		} else {
			voteToHalt();
		}
	}

}
