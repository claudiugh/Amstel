package nl.vu.cs.amstel.user;

import java.util.Iterator;

import org.apache.log4j.Logger;

public abstract class Combiner<M extends MessageValue> {
	
	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	protected M output;
	
	public abstract M combine(M a, M b);
	
	public Combiner(M outputInstance) {
		output = outputInstance;
	}
	
	public M combine(Iterable<M> msgs) {
		Iterator<M> iterator = msgs.iterator();
		if (iterator.hasNext()) {
			logger.error("Empty iterator given for combining");
			return null;
		}
		M first = msgs.iterator().next();
		while (iterator.hasNext()) {
			M m = iterator.next();
			output = combine(m, first);
			first.copy(output);
		}
			
		return output;
	}
	
}
