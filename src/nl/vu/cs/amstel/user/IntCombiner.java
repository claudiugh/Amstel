package nl.vu.cs.amstel.user;

import org.apache.log4j.Logger;

public abstract class IntCombiner extends Combiner<IntMessage> {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	public IntCombiner() {
		super(new IntMessage());
	}

	abstract protected int combineValues(int a, int b);

	public IntMessage combine(IntMessage a, IntMessage b) {
		output.value = combineValues(a.value, b.value);
		return output;
	}
}
