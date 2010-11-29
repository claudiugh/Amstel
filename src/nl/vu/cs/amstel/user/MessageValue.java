package nl.vu.cs.amstel.user;

import java.io.IOException;

import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

public interface MessageValue {

	public void serialize(WriteMessage w) throws IOException;
	
	public void deserialize(ReadMessage r) throws IOException;
	
}
