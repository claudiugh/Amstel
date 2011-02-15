package nl.vu.cs.amstel.user;

public abstract class IntAggregator extends Aggregator<IntValue> {

	public IntAggregator(String name) {
		super(IntValue.class, name);
	}

	public abstract int combineValues(int a, int b);
	
	@Override
	public void combine(IntValue value) {
		this.value.value = combineValues(value.value, this.value.value);
	}

	@Override
	public void init(IntValue value) {
		this.value = new IntValue(value.value);
	}

}
