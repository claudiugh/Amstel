package nl.vu.cs.amstel.examples;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.user.IntMessage;
import nl.vu.cs.amstel.user.IntValue;
import nl.vu.cs.amstel.user.NullValue;
import nl.vu.cs.amstel.user.Vertex;

public class MaxvalVertex extends Vertex<IntValue, NullValue, IntMessage> {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	public void compute(Iterable<IntMessage> messages) {
		IntValue local = getValue();
		IntMessage outMsg = newMessage();
		if (getSuperstep() == 0) {
			outMsg.value = local.value;
			sendToAll(outMsg);
			return;
		}

		int max = local.value;
		for (IntMessage m : messages) {
			if (m.value > max) {
				max = m.value;
			}
		}
		if (local.value != max) {
			// we found a better value
			local.value = max;
			outMsg.value = max;
			sendToAll(outMsg);
		} else {
			voteToHalt();
		}
	}

}
