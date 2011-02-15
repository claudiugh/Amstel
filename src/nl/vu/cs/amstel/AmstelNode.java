package nl.vu.cs.amstel;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.amstel.user.Aggregator;
import nl.vu.cs.amstel.user.Combiner;
import nl.vu.cs.amstel.user.MessageValue;
import nl.vu.cs.amstel.user.Value;

public abstract class AmstelNode<V, M extends MessageValue> {
	
	protected Map<String, AggregatorState> aggregators =
		new HashMap<String, AggregatorState>();
	
	public abstract void run() throws Exception;
	
	public abstract void setCombiner(Class<? extends Combiner<M>> combinerClass);
	
	public void addAggregator(Aggregator<? extends Value> aggregator) {
		aggregators.put(aggregator.getName(), new AggregatorState(aggregator));
	}
}
