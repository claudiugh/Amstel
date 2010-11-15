package nl.vu.cs.amstel.msg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.amstel.Node;
import nl.vu.cs.amstel.VertexState;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class MessageRouter extends Thread {

	private Ibis ibis;
	private IbisIdentifier[] partitions;
	private Map<String, VertexState> vertexes;

	// the send ports for each other worker
	private Map<IbisIdentifier, SendPort> senders = 
		new HashMap<IbisIdentifier, SendPort>();
	
	private SendPort getSender(IbisIdentifier worker) throws IOException {
		if (!senders.containsKey(worker)) {
			SendPort sender = ibis.createSendPort(Node.W2W_PORT);
			sender.connect(worker, "worker");
			senders.put(worker, sender);
		}
		return senders.get(worker);
	}
	
	private IbisIdentifier getOwner(String vid) {
		return partitions[vid.hashCode() % partitions.length];
	}
	
	public MessageRouter(Ibis ibis, IbisIdentifier[] partitions, 
			Map<String, VertexState> vertexes) {
		this.ibis = ibis;
		this.partitions = partitions;
		this.vertexes = vertexes;
	}
	
	public void send(VertexState vertex) throws IOException {
		IbisIdentifier owner = getOwner(vertex.getID());
		if (owner.equals(ibis.identifier())) {
			// just add to the local collection
			vertexes.put(vertex.getID(), vertex);
		} else {
			// send to the owner
			SendPort sender = getSender(owner);
			WriteMessage w = sender.newMessage();
			vertex.serialize(w);
			w.finish();
		}		
	}
	
	public void run() {
		
	}
	
}
