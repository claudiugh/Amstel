package nl.vu.cs.amstel;

import nl.vu.cs.amstel.user.Combiner;
import nl.vu.cs.amstel.user.MessageValue;

public interface AmstelNode<V, M extends MessageValue> {
	
	public void run() throws Exception;
	
	public void setCombiner(Class<? extends Combiner<M>> combinerClass);
}
