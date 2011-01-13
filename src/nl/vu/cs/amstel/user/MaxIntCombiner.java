package nl.vu.cs.amstel.user;

public class MaxIntCombiner extends IntCombiner {

	@Override
	protected int combineValues(int a, int b) {
		return a < b ? b : a;
	}

}
