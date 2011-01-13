package nl.vu.cs.amstel.user;

public class SumIntCombiner extends IntCombiner {

	@Override
	protected int combineValues(int a, int b) {
		return a + b;
	}

}
