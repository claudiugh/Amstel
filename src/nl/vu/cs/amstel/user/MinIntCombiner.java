package nl.vu.cs.amstel.user;

public class MinIntCombiner extends IntCombiner {

	@Override
	protected int combineValues(int a, int b) {
		return a < b ? a : b;
	}

	
}
