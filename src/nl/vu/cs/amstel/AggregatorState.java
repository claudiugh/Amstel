package nl.vu.cs.amstel;

import nl.vu.cs.amstel.user.Aggregator;
import nl.vu.cs.amstel.user.Value;

public class AggregatorState {

	public Aggregator<?> aggregator;
	public Value currentValue = null;
	
	public AggregatorState(Aggregator<?> aggregator) {
		this.aggregator = aggregator;
	}
}
