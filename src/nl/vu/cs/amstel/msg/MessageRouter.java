package nl.vu.cs.amstel.msg;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.Node;
import nl.vu.cs.amstel.VertexState;
import nl.vu.cs.amstel.user.MessageValue;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class MessageRouter<M extends MessageValue> {

	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private Ibis ibis;
	private IbisIdentifier[] partitions;
	private Map<String, VertexState<M>> vertexes;
	
	// the send ports for each other worker
	private Map<IbisIdentifier, SendPort> senders = 
		new HashMap<IbisIdentifier, SendPort>();
	// the send buffers for each worker
	private Map<IbisIdentifier, OutgoingQueue<M>> buffers =
		new HashMap<IbisIdentifier, OutgoingQueue<M>>();
	// active worker channels
	private Set<IbisIdentifier> activeWorkers =
		Collections.synchronizedSet(new HashSet<IbisIdentifier>());
	
	private IbisIdentifier getOwner(String vid) {
		int hash = vid.hashCode();
		if (hash < 0) {
			hash = -hash;
		}
		return partitions[hash % partitions.length];
	}
	
	public MessageRouter(Ibis ibis, IbisIdentifier[] partitions, 
			Map<String, VertexState<M>> vertexes) {
		this.ibis = ibis;
		this.partitions = partitions;
		this.vertexes = vertexes;
	}
	
	public synchronized void deactivateWorker(IbisIdentifier worker) {
		activeWorkers.remove(worker);
		if (activeWorkers.size() == 0) {
			notify();
		}
	}
	
	private synchronized void waitFlushAck() {
		if (activeWorkers.size() > 0) {
			logger.info("Channels to be acked: " + activeWorkers);
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void activateWorker(IbisIdentifier worker) {
		if (!activeWorkers.contains(worker)) {
			activeWorkers.add(worker);
		}
	}
	
	public synchronized SendPort getSender(IbisIdentifier worker) throws IOException {
		if (!senders.containsKey(worker)) {
			SendPort sender = ibis.createSendPort(Node.W2W_PORT);
			sender.connect(worker, "worker");
			senders.put(worker, sender);
			buffers.put(worker, new OutgoingQueue<M>());
		}
		return senders.get(worker);
	}
	
	public void send(VertexState<M> vertex) throws IOException {
		IbisIdentifier owner = getOwner(vertex.getID());
		if (owner.equals(ibis.identifier())) {
			// just add to the local collection
			vertexes.put(vertex.getID(), vertex);
		} else {
			// send to the owner
			SendPort sender = getSender(owner);
			synchronized(sender) {
				WriteMessage w = sender.newMessage();
				w.writeInt(MessageReceiver.INPUT_MSG);
				vertex.serialize(w);
				w.finish();
			}
			activateWorker(owner);
		}
	}
	
	private void sendBuffer(IbisIdentifier worker, OutgoingQueue<M> buffer) throws IOException {
		SendPort sender = getSender(worker);
		synchronized(sender) {
			WriteMessage w = sender.newMessage();
			w.writeInt(MessageReceiver.COMPUTE_MSG);
			buffer.sendBulk(w);
			w.finish();
		}
		activateWorker(worker);
	}
	
	public void send(String toVertex, M msg) throws IOException {
		IbisIdentifier owner = getOwner(toVertex);
		if (owner.equals(ibis.identifier())) {
			// deliver locally
			vertexes.get(toVertex).deliver(msg);
		} else {
			// enqueue for sending
			OutgoingQueue<M> buffer = buffers.get(owner);
			buffer.add(toVertex, msg);
			if (buffer.reachedThreshold()) {
				sendBuffer(owner, buffer);
			}
		}
	}
	
	private void sendFlush(IbisIdentifier worker) throws IOException {
		SendPort sender = getSender(worker);
		synchronized(sender) {
			WriteMessage w = sender.newMessage();
			w.writeInt(MessageReceiver.FLUSH_MSG);
			w.finish();
		}
	}
	
	/**
	 * sends all the buffered messages and will block until the acknowledgments
	 * for the flush messages are received
	 * @throws IOException
	 */
	public void flush() throws IOException {
		for (IbisIdentifier worker : senders.keySet()) {
			OutgoingQueue<M> buffer = buffers.get(worker);
			if (buffer.getCount() > 0) {
				sendBuffer(worker, buffer);
			}
			if (activeWorkers.contains(worker)) {
				sendFlush(worker);
			}
		}
		waitFlushAck();
	}
	
	public void close() throws IOException {
		for (IbisIdentifier worker : senders.keySet()) {
			getSender(worker).close();
		}
	}
	
}
