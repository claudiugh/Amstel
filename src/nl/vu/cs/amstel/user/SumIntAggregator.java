package nl.vu.cs.amstel.user;

public class SumIntAggregator extends IntAggregator {

	public SumIntAggregator(String name) {
		super(name);
	}

	@Override
	public int combineValues(int a, int b) {
		return a + b;
	}

}
