package nl.vu.cs.amstel.user;

public class MaxIntAggregator extends IntAggregator {

	public MaxIntAggregator(String name) {
		super(name);
	}

	@Override
	public int combineValues(int a, int b) {
		return a > b ? a : b;
	}

}
