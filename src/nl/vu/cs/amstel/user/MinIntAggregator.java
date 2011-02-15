package nl.vu.cs.amstel.user;

public class MinIntAggregator extends IntAggregator {

	public MinIntAggregator(String name) {
		super(name);
	}

	@Override
	public int combineValues(int a, int b) {
		return a < b ? a : b;
	}

}
