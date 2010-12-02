package nl.vu.cs.amstel.examples;


import nl.vu.cs.amstel.msg.MessageIterator;
import nl.vu.cs.amstel.user.IntMessage;
import nl.vu.cs.amstel.user.Vertex;

public class MaxvalVertex extends Vertex<IntMessage> {

	public void compute(MessageIterator<IntMessage> messages) {
		IntMessage outMsg = newMessage();
		if (getSuperstep() == 0) {
			outMsg.value = getValue();
			sendToAll(outMsg);
			return;
		}
		int max = getValue();
		//System.out.println(this + ": " + VertexState.msgStats(messages));
		for (IntMessage m : messages) {
			if (m.value > max) {
				max = m.value;
			}
		}
		if (max != getValue()) {
			setValue(max);
			outMsg.value = max;
			sendToAll(outMsg);
		} else {
			voteToHalt();
		}
	}

}
