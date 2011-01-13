package nl.vu.cs.amstel.user;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class IntMessage implements MessageValue {
	public int value;
	
	public IntMessage() {
	}
	
	public IntMessage(int value) {
		this.value = value;
	}
	
	public void serialize(DataOutputStream out) throws IOException {
		out.writeInt(value);
	}
	
	public void deserialize(DataInputStream in) throws IOException {
		this.value = in.readInt();
	}
	
	public String toString() {
		return "" + value;
	}

	@Override
	public <M extends MessageValue> void copy(M other) {
		IntMessage m = (IntMessage) other;
		value = m.value;
	}
}
