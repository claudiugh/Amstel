package nl.vu.cs.amstel.examples;

import nl.vu.cs.amstel.graph.OutEdgeIterator;
import nl.vu.cs.amstel.user.IntMessage;
import nl.vu.cs.amstel.user.IntValue;
import nl.vu.cs.amstel.user.Vertex;

public class SSSPVertex extends Vertex<IntValue, IntValue, IntMessage> {

	private static final String SRC = "V0";
	private static final String DST = "V50";
	
	@Override
	public void compute(Iterable<IntMessage> messages) {
		int mindist = (getID().equals(SRC)) ? 0 : Integer.MAX_VALUE;
		for (IntMessage m : messages) {
			if (m.value < mindist) {
				mindist = m.value;
			}
		}
		IntValue dist = getValue();
		if (mindist < Integer.MAX_VALUE && mindist < dist.value) {
			dist.value = mindist;
			OutEdgeIterator<IntValue> iter = getOutEdgeIterator();
			while (iter.hasNext()) {
				IntMessage m = newMessage();
				m.value = mindist + iter.getEdgeValue().value;
				send(iter.getEdgeTarget(), m);
				iter.next();
			}
			if (getID().equals(DST)) {
				outputAggregate("Destination", dist);
			}
		}
		voteToHalt();
	}
	
}
