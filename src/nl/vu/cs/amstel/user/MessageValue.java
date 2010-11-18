package nl.vu.cs.amstel.user;

import java.io.IOException;

import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

public class MessageValue {
	public int value;
	
	public MessageValue() {
	}
	
	public MessageValue(int value) {
		this.value = value;
	}
	
	public void serialize(WriteMessage w) throws IOException {
		w.writeInt(value);
	}
	
	public void deserialize(ReadMessage r) throws IOException {
		this.value = r.readInt();
	}
}
