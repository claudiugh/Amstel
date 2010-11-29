package nl.vu.cs.amstel.examples;

import java.util.List;

import nl.vu.cs.amstel.user.IntMessage;
import nl.vu.cs.amstel.user.Vertex;

public class MaxvalVertex extends Vertex<IntMessage> {

	public void compute(List<IntMessage> messages) {
		if (getSuperstep() == 0) {
			sendToAll(new IntMessage(getValue()));
			return;
		}
		int max = getValue();
		for (IntMessage m : messages) {
			if (m.value > max) {
				max = m.value;
			}
		}
		if (max != getValue()) {
			setValue(max);
			sendToAll(new IntMessage(getValue()));
		} else {
			voteToHalt();
		}
	}

}
