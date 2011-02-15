package nl.vu.cs.amstel.user;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Marker class for unused values. This is very useful for edge values, where
 * the number of values is huge.
 *  
 * @author claudiugh
 */
public class NullValue implements Value {

	@Override
	public void deserialize(DataInputStream in) throws IOException {
	}

	@Override
	public void serialize(DataOutputStream out) throws IOException {
	}

}
