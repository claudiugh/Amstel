package nl.vu.cs.amstel.msg;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.Node;
import nl.vu.cs.amstel.graph.VertexState;
import nl.vu.cs.amstel.user.MessageValue;
import nl.vu.cs.amstel.user.Value;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class MessageRouter<V extends Value, E extends Value,
		M extends MessageValue> {

	public static final int INPUT_VERTEX_BUFFER = 40960;
	
	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	protected Ibis ibis;
	protected MessageFactory<M> msgFactory;
	
	private IbisIdentifier[] partitions;
	private Map<String, VertexState<V, E>> vertices;
	private InboundQueue<M> inbox = null;
	private HashMap<IbisIdentifier, ExposedByteArrayOutputStream> vertexBuffer =
		new HashMap<IbisIdentifier, ExposedByteArrayOutputStream>();
	private HashMap<IbisIdentifier, DataOutputStream> vertexOutStream =
		new HashMap<IbisIdentifier, DataOutputStream>();
	
	
	// the send ports for each other worker
	private Map<IbisIdentifier, SendPort> senders = 
		new HashMap<IbisIdentifier, SendPort>();
	// the send buffers for each worker
	private Map<IbisIdentifier, OutgoingQueue<M>> outQueues =
		new HashMap<IbisIdentifier, OutgoingQueue<M>>();
	// active worker channels
	private Set<IbisIdentifier> activeWorkers =
		Collections.synchronizedSet(new HashSet<IbisIdentifier>());
	
	public MessageRouter(Ibis ibis, IbisIdentifier[] partitions, 
			Map<String, VertexState<V, E>> vertices, 
			MessageFactory<M> msgFactory) {
		this.ibis = ibis;
		this.partitions = partitions;
		this.vertices = vertices;
		this.msgFactory = msgFactory;
	}
	
	public IbisIdentifier getOwner(String vid) {
		int hash = vid.hashCode();
		if (hash < 0) {
			hash = -hash;
		}
		return partitions[hash % partitions.length];
	}
	
	public void setInbox(InboundQueue<M> inbox) {
		this.inbox = inbox;
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
	
	private OutgoingQueue<M> createOutgoingQueue() {
		if (!msgFactory.hasCombiner()) {
			return new SerializedOutgoingQueue<M>();
		}
		// else create the combined version
		return new CombinedOutgoingQueue<M>(msgFactory);
	}
	
	private OutgoingQueue<M> getQueue(IbisIdentifier worker) {
		if (!outQueues.containsKey(worker)) {
			outQueues.put(worker, createOutgoingQueue());
		}
		return outQueues.get(worker);
	}
	
	public synchronized SendPort getSender(IbisIdentifier worker) throws IOException {
		if (!senders.containsKey(worker)) {
			SendPort sender = ibis.createSendPort(Node.W2W_PORT);
			sender.connect(worker, "worker");
			senders.put(worker, sender);
		}
		return senders.get(worker);
	}
	
	private DataOutputStream getVertexStream(IbisIdentifier worker) {
		if (!vertexOutStream.containsKey(worker)) {
			ExposedByteArrayOutputStream buffer =
				new ExposedByteArrayOutputStream(INPUT_VERTEX_BUFFER);
			vertexBuffer.put(worker, buffer);
			vertexOutStream.put(worker, new DataOutputStream(buffer));
		}
		return vertexOutStream.get(worker);
	}
	
	private void flushInputVertices(IbisIdentifier owner) throws IOException {
		SendPort sender = getSender(owner);
		ExposedByteArrayOutputStream buffer = vertexBuffer.get(owner);
		synchronized(sender) {
			WriteMessage w = sender.newMessage();
			w.writeInt(MessageReceiver.INPUT_MSG);
			w.writeInt(buffer.size());
			w.writeArray(buffer.getBuffer(), 0, buffer.size());
			w.finish();
		}
		buffer.reset();	
	}
	
	public void flushInputVertices() throws IOException {
		for (IbisIdentifier worker : vertexBuffer.keySet()) {
			if (vertexBuffer.get(worker).size() > 0) {
				flushInputVertices(worker);
			}
			activeWorkers.add(worker);
			sendFlush(worker);
		}
		waitFlushAck();
	}
	
	public void send(VertexState<V, E> vertex) throws IOException {
		IbisIdentifier owner = getOwner(vertex.getID());
		DataOutputStream vertexOutStream = getVertexStream(owner);
		vertex.serialize(vertexOutStream);
		if (vertexBuffer.get(owner).size() > INPUT_VERTEX_BUFFER - 4096) {
			flushInputVertices(owner);		
		}
	}
	
	private void sendQueue(IbisIdentifier worker, OutgoingQueue<M> outQueue, 
						boolean flush) throws IOException {
		SendPort sender = getSender(worker);
		synchronized(sender) {
			WriteMessage w = sender.newMessage();
			w.writeInt(MessageReceiver.COMPUTE_MSG);
			if (flush) {
				outQueue.flush(w);
			} else {
				outQueue.flushFilledBuffers(w);
			}
			w.finish();
		}
		activateWorker(worker);
	}
	
	public void send(String toVertex, M msg) throws IOException {
		IbisIdentifier owner = getOwner(toVertex);
		if (owner.equals(ibis.identifier())) {
			// deliver locally
			inbox.deliverLocally(vertices.get(toVertex).getIndex(), msg);
		} else {
			// enqueue for sending
			OutgoingQueue<M> outQueue = getQueue(owner);
			outQueue.add(toVertex, msg);
			if (outQueue.reachedThreshold()) {
				sendQueue(owner, outQueue, false);
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
		for (IbisIdentifier worker : outQueues.keySet()) {
			OutgoingQueue<M> outQueue = getQueue(worker);
			if (!outQueue.isEmpty()) {
				sendQueue(worker, outQueue, true);
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
