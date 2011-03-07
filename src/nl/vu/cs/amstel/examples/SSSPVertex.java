package nl.vu.cs.amstel.examples;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.graph.OutEdgeIterator;
import nl.vu.cs.amstel.user.IntMessage;
import nl.vu.cs.amstel.user.IntValue;
import nl.vu.cs.amstel.user.Vertex;

public class SSSPVertex extends Vertex<IntValue, IntValue, IntMessage> {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private int SP_TH = 1000;
	
	// these must be initialized before the computation begins
	public static String SRC = null;
	public static String DST = null;
	
	private static IntValue outdegree = new IntValue();
	
	@Override
	public void compute(Iterable<IntMessage> messages) {
		if (getSuperstep() == 0) {
			outdegree.value = getOutdegree();
			outputAggregate("Max-Outdegree", outdegree);
			getValue().value = Integer.MAX_VALUE;
			return;
		} else if (getSuperstep() > SP_TH) {
			voteToHalt();
			return;
		}
		
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
