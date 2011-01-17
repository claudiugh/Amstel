package nl.vu.cs.amstel.user;


public interface MessageValue extends Value {

	public <M extends MessageValue> void copy(M other);
}
