package nl.vu.cs.amstel;

import java.util.List;

class MaxValMessage {
	public int max;
	
	public MaxValMessage(int max) {
		this.max = max;
	}
}

class MaxValueVertex extends Vertex<MaxValMessage> {

	private void sendToAll() {
		for (Integer v : getOutEdges()) {
			send(v, new MaxValMessage(getValue()));
		}		
	}
	
	public void compute(List<Message<MaxValMessage>> messages) {
		log(" my value is " + getValue());
		// initial iteration
		if (getSuperstep() == 0) {
			sendToAll();
			return;
		}
		int max = getValue();
		// get the maximum from the received messages
		for (Message<MaxValMessage> m : messages) {
			MaxValMessage msg = m.payload;
			if (msg.max > max) {
				max = msg.max;
			}
		}
		if (max != getValue()) {
			setValue(max);
			sendToAll();
		} else {
			voteToHalt();
		}
	}

}

public class Runner {

	public static void main(String[] args) {
		// instantiate the Master and the Workers
		Master m = new Master(3, MaxValueVertex.class);
		m.start();
		try {
			m.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
