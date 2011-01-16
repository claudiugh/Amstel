package nl.vu.cs.amstel.user;

import nl.vu.cs.amstel.Value;

public interface MessageValue extends Value {

	public <M extends MessageValue> void copy(M other);
}
