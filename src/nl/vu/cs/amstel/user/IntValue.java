package nl.vu.cs.amstel.user;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class IntValue implements Value {
	public int value = 0;
	
	public IntValue() {
	}
	
	public IntValue(int initial) {
		value = initial;
	}

	public IntValue(String initial) {
		try {
			value = Integer.parseInt(initial);
		} catch (NumberFormatException e) {
			value = 0;
		}
	}
	
	@Override
	public void deserialize(DataInputStream in) throws IOException {
		value = in.readInt();
	}

	@Override
	public void serialize(DataOutputStream out) throws IOException {
		out.writeInt(value);
	}
}
