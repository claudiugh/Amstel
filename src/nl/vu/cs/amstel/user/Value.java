package nl.vu.cs.amstel.user;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Value {

	public void serialize(DataOutputStream out) throws IOException;
	
	public void deserialize(DataInputStream in) throws IOException;

}
