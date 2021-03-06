package nl.vu.cs.amstel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.graph.ArrayOutEdgeIterator;
import nl.vu.cs.amstel.graph.VertexFactory;
import nl.vu.cs.amstel.graph.VertexState;
import nl.vu.cs.amstel.graph.io.InputPartition;
import nl.vu.cs.amstel.graph.io.Reader;
import nl.vu.cs.amstel.msg.CombinedInboundQueue;
import nl.vu.cs.amstel.msg.InboundQueue;
import nl.vu.cs.amstel.msg.MessageFactory;
import nl.vu.cs.amstel.msg.MessageIterator;
import nl.vu.cs.amstel.msg.MessageReceiver;
import nl.vu.cs.amstel.msg.MessageRouter;
import nl.vu.cs.amstel.msg.SerializedInboundQueue;
import nl.vu.cs.amstel.user.Combiner;
import nl.vu.cs.amstel.user.MessageValue;
import nl.vu.cs.amstel.user.Value;
import nl.vu.cs.amstel.user.Vertex;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class Worker<V extends Value, E extends Value, M extends MessageValue> 
		extends AmstelNode<V, M> {


	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	// for instantiation of user provided classes
	private Class<? extends Vertex<V, E, M>> vertexClass;
	private MessageFactory<M> messageFactory;
	private VertexFactory<V, E> vertexFactory; 
	
	private Ibis ibis;
	private IbisIdentifier master;
	private SendPort masterSender;
	private ReceivePort masterReceiver;
	private ReceivePort receiver;
	private WorkerBarrier barrier;
	private IbisIdentifier[] partitions;
	private Reader reader;
	private InputPartition inputPartition;
	
	// vertex states
	private int crtIndex = -1;
	private Map<Integer, String> idToVertex =
		new HashMap<Integer, String>();
	private boolean[] active;
	
	private Map<String, VertexState<V, E>> vertices = 
		new HashMap<String, VertexState<V, E>>();
	// messages in-boxes
	private InboundQueue<M> inbox;
	private InboundQueue<M> futureInbox;
	
	// messaging entities
	private MessageReceiver<V, E, M> messageReceiver;
	private MessageRouter<V, E, M> messageRouter;
	
	// state of the computation
	private WorkerState<E, M> state;
	
	@SuppressWarnings("unchecked")
	private void register() throws IOException {
		WriteMessage w = masterSender.newMessage();
		w.writeString("register");
		w.finish();
		ReadMessage r = masterReceiver.receive();
		try {
			partitions = (IbisIdentifier[]) r.readObject();
			Map<IbisIdentifier, InputPartition> inputPartitions = 
				(Map<IbisIdentifier, InputPartition>) r.readObject();
			inputPartition = inputPartitions.get(ibis.identifier());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		r.finish();
	}
	
	private void addVertex(VertexState<V, E> vertex) {
		crtIndex++;
		vertex.setIndex(crtIndex);
		vertices.put(vertex.getID(), vertex);
		idToVertex.put(vertex.getIndex(), vertex.getID());
	}
	
	private void readInput() throws Exception {
		reader.init(inputPartition);
		int readVertices = 0;
		while (reader.hasNext()) {
			VertexState<V, E> vertexState = reader.nextVertex(vertexFactory);
			if (messageRouter.getOwner(vertexState.getID()).equals(
					ibis.identifier())) {
				// this vertex belongs to me
				addVertex(vertexState);
			} else {
				messageRouter.send(vertexState);	
			}
			readVertices++;
		}
		
		logger.info("Read " + readVertices + " vertices and " + reader);
		reader.close();
		messageRouter.flushInputVertices();
	}
	
	private void loadReceivedInput() {
		for (VertexState<V, E> vertex : messageReceiver.getReceivedVertexes()) {
			addVertex(vertex);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void initInboxes() {
		int count = crtIndex + 1;
		active = new boolean[count];
		
		if (!messageFactory.hasCombiner()) {
			MessageIterator<M> msgIterator = 
				new MessageIterator<M>(messageFactory.create());
			inbox = new SerializedInboundQueue(count, vertices, idToVertex,
					msgIterator);
			futureInbox = new SerializedInboundQueue(count, vertices, 
					idToVertex, msgIterator);
			for (int i = 0; i < count; i++) {
				active[i] = true;
			}
		} else {
			inbox = new CombinedInboundQueue(count, messageFactory, vertices);
			futureInbox = new CombinedInboundQueue(count, messageFactory,
					vertices);
			for (int i = 0; i < count; i++) {
				active[i] = true;
			}
		}

		state.active = active;
	}
	
	private void setupWorkerConnections() throws IOException {
		receiver = ibis.createReceivePort(Node.W2W_PORT, "worker");
		receiver.enableConnections();
		// this is a thread that only listens for incoming messages
		messageRouter = new MessageRouter<V, E, M>(ibis, partitions, vertices,
				messageFactory);
		messageReceiver = new MessageReceiver<V, E, M>(receiver, messageRouter, 
				vertexFactory);
		messageReceiver.start();
		state = new WorkerState<E, M>(messageRouter, aggregators);
	}
	
	private void closeWorkerConnections() throws IOException {
		messageRouter.close();
		// this will cause the receiver thread to exit
		receiver.close();
		try {
			messageReceiver.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void computeVertices(Vertex<V, E, M> v, MessageIterator<M> msgIterator) 
			throws IOException {
		int msgs = 0;
		for (int i = 0; i < active.length; i++) {
			if (inbox.hasMessages(i) || active[i]) {
				VertexState<V, E> vertexState = 
					vertices.get(idToVertex.get(i));
				v.setState(vertexState);
				// we consider the vertex as active
				active[i] = true;
				vertexState.setEdgeIterator(state.edgeIterator);
				v.compute(inbox.getIterator(i));
				// clear all messages
				inbox.clear(i);
			}
			if (inbox.hasMessages(i)) {
				msgs++;
			}
		}
	}
	
	private int countActiveVertices() {
		int count = 0;
		for (int i = 0; i < active.length; i++) {
			if (futureInbox.hasMessages(i) || active[i]) {
				count++;
			}
		}
		return count;
	}
	
	private void swapInboxes() {
		InboundQueue<M> tmp = inbox;
		inbox = futureInbox;
		futureInbox = tmp;
	}
	
	private void resetAggregators() {
		for (AggregatorState aggState : aggregators.values()) {
			aggState.aggregator.reset();
		}
	}
	
	@Override
	public void setCombiner(Class<? extends Combiner<M>> combinerClass) {
		messageFactory.setCombinerClass(combinerClass);
	}
	
	public void run() throws Exception, InterruptedException {
		logger.info("I am " + ibis.identifier());
		register();
		setupWorkerConnections();
		// reading input and distribute vertexes
		readInput();
		// we synchronize here because we need to be sure that 
		// all the input messages have been received
		barrier.enter();
		loadReceivedInput();
		// initialize vertex arrays
		initInboxes();

		state.activeVertices = vertices.size();
		state.msg = messageFactory.create();
		state.edgeIterator = new ArrayOutEdgeIterator<E>();
		// instantiate the vertex handler and the message iterator 
		Vertex<V, E, M> v;
		MessageIterator<M> msgIterator =
			new MessageIterator<M>(messageFactory.create());
		try {
			v = vertexClass.newInstance();
			v.setWorkerState(state);
			// the computation iteration
			while (state.superstep >= 0) {
				messageReceiver.setInbox(futureInbox);
				messageRouter.setInbox(futureInbox);
				state.superstep = barrier.enterAndGetData(state.activeVertices);
				if (state.superstep >= 0) {
					// compute phase
					resetAggregators();
					logger.info("Running superstep " + state.superstep);
					computeVertices(v, msgIterator);				
					// send everything left in the buffers 
					// and wait for acknowledgments
					messageRouter.flush();					
				}
				
				// cool-down phase of the super-step
				// in this phase no one is sending/receiving message, so it's
				// safe to switch the message in-boxes 
				barrier.enterCooldown();
				// the current in-box is supposed to be already processed,
				// and it's going to be the futureInbox in the next super-step
				state.activeVertices = countActiveVertices();
				swapInboxes();
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
				
		// close connections
		closeWorkerConnections();
		// exit
		masterSender.close();
		masterReceiver.close();
		ibis.end();
	}
	
	public Worker(Ibis ibis, IbisIdentifier master, 
			Class<? extends Vertex<V, E, M>> vertexClass,
			Class<V> vertexValueClass,
			Class<E> edgeValueClass,
			Class<M> messageClass,
			Reader reader)
		throws IOException, InterruptedException {
		// setup
		this.ibis = ibis;
		this.reader = reader;
		this.master = master;
		this.vertexClass = vertexClass;
		messageFactory = new MessageFactory<M>(messageClass);
		vertexFactory = new VertexFactory<V, E>(vertexValueClass, 
				edgeValueClass);
		masterSender = ibis.createSendPort(Node.W2M_PORT);
		masterSender.connect(master, "w2m");
		masterReceiver = ibis.createReceivePort(Node.M2W_PORT, "m2w");
		masterReceiver.enableConnections();
		barrier = new WorkerBarrier(masterSender, masterReceiver, 
			new AggregatorStream(aggregators));
	}

}
