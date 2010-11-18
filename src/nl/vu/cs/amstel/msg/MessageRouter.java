package nl.vu.cs.amstel.msg;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import nl.vu.cs.amstel.Node;
import nl.vu.cs.amstel.VertexState;
import nl.vu.cs.amstel.user.MessageValue;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class MessageRouter {

	private Ibis ibis;
	private IbisIdentifier[] partitions;
	private Map<String, VertexState> vertexes;
	
	
	private int packetNo = 0; 
	private Set<Integer> packets = Collections.synchronizedSet(
		new HashSet<Integer>());
	// the send ports for each other worker
	private Map<IbisIdentifier, SendPort> senders = 
		new HashMap<IbisIdentifier, SendPort>();
	// the send buffers for each worker
	private Map<IbisIdentifier, OutgoingQueue> buffers =
		new HashMap<IbisIdentifier, OutgoingQueue>();
	
	private IbisIdentifier getOwner(String vid) {
		return partitions[vid.hashCode() % partitions.length];
	}
	
	private int nextPacketNo() {
		packetNo = (packetNo + 1) % Integer.MAX_VALUE;
		return packetNo;
	}
	
	private void addPacket(int packetNo) {
		packets.add(packetNo);
	}
	
	public MessageRouter(Ibis ibis, IbisIdentifier[] partitions, 
			Map<String, VertexState> vertexes) {
		this.ibis = ibis;
		this.partitions = partitions;
		this.vertexes = vertexes;
	}
	
	public synchronized void ackPacket(int packetNo) {
		System.out.println("Ack for packet " + packetNo + " - " + packets);
		packets.remove(packetNo);
		if (packets.size() == 0) {
			notify();
		}
	}
	
	public synchronized void waitForAck() throws InterruptedException {
		if (packets.size() > 0) {
			System.out.println("Packets to be acked: " + packets);
			wait();
		}
	}
	
	public synchronized SendPort getSender(IbisIdentifier worker) throws IOException {
		if (!senders.containsKey(worker)) {
			SendPort sender = ibis.createSendPort(Node.W2W_PORT);
			sender.connect(worker, "worker");
			senders.put(worker, sender);
			buffers.put(worker, new OutgoingQueue());
		}
		return senders.get(worker);
	}
	
	public void send(VertexState vertex) throws IOException {
		IbisIdentifier owner = getOwner(vertex.getID());
		if (owner.equals(ibis.identifier())) {
			// just add to the local collection
			vertexes.put(vertex.getID(), vertex);
		} else {
			// send to the owner
			System.out.println("Sending " + vertex.getID());
			SendPort sender = getSender(owner);
			synchronized(sender) {
				WriteMessage w = sender.newMessage();
				w.writeInt(MessageReceiver.INPUT_MSG);
				int packetNo = nextPacketNo();
				addPacket(packetNo);
				w.writeInt(packetNo);
				vertex.serialize(w);
				w.finish();
			}
		}
	}
	
	private void sendBuffer(IbisIdentifier worker, OutgoingQueue buffer) throws IOException {
		SendPort sender = getSender(worker);
		synchronized(sender) {
			WriteMessage w = sender.newMessage();
			w.writeInt(MessageReceiver.COMPUTE_MSG);
			int packetNo = nextPacketNo();
			addPacket(packetNo);
			w.writeInt(packetNo);
			buffer.sendBulk(w);
			w.finish();
		}		
	}
	
	public void send(String toVertex, MessageValue msg) throws IOException {
		IbisIdentifier owner = getOwner(toVertex);
		if (owner.equals(ibis.identifier())) {
			// deliver locally
			vertexes.get(toVertex).deliver(msg);
		} else {
			// enqueue for sending
			OutgoingQueue buffer = buffers.get(owner);
			buffer.add(toVertex, msg);
			if (buffer.reachedThreshold()) {
				sendBuffer(owner, buffer);
			}
		}
	}
	
	public void flush() throws IOException {
		for (IbisIdentifier worker : senders.keySet()) {
			OutgoingQueue buffer = buffers.get(worker);
			if (buffer.getCount() > 0) {
				sendBuffer(worker, buffer);
			}
		}
	}
	
}
