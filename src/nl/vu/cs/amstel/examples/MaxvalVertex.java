package nl.vu.cs.amstel.examples;

import nl.vu.cs.amstel.user.IntMessage;
import nl.vu.cs.amstel.user.IntValue;
import nl.vu.cs.amstel.user.NullValue;
import nl.vu.cs.amstel.user.Vertex;

public class MaxvalVertex extends Vertex<IntValue, NullValue, IntMessage> {

	private static boolean printed = false;
	
	public void compute(Iterable<IntMessage> messages) {
		IntValue max = getValue();
		IntMessage outMsg = newMessage();
		if (getSuperstep() == 0) {
			outputAggregate("MaxVertex", max);
			outMsg.value = max.value;
			sendToAll(outMsg);
			return;
		}
		if (!printed && getSuperstep() == 1) {
			System.out.println("MaxVertex value: " + readAggregate("MaxVertex"));
			printed = true;
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
