package nl.vu.cs.amstel.examples;

import java.util.List;

import nl.vu.cs.amstel.user.MessageValue;
import nl.vu.cs.amstel.user.Vertex;

public class MaxvalVertex extends Vertex {

	public void compute(List<MessageValue> messages) {
		System.out.println("Running compute for " + this + " " + messages);
		if (getSuperstep() == 0) {
			sendToAll(new MessageValue(getValue()));
			return;
		}
		int max = getValue();
		for (MessageValue m : messages) {
			if (m.value > max) {
				max = m.value;
			}
		}
		if (max != getValue()) {
			setValue(max);
			sendToAll(new MessageValue(getValue()));
		} else {
			voteToHalt();
		}
	}

}
