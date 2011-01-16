package nl.vu.cs.amstel;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.graph.GraphInput;
import nl.vu.cs.amstel.graph.InputPartition;
import nl.vu.cs.amstel.graph.VertexValueFactory;
import nl.vu.cs.amstel.msg.CombinedInboundQueue;
import nl.vu.cs.amstel.msg.InboundQueue;
import nl.vu.cs.amstel.msg.MessageFactory;
import nl.vu.cs.amstel.msg.MessageIterator;
import nl.vu.cs.amstel.msg.MessageOutputBuffer;
import nl.vu.cs.amstel.msg.MessageReceiver;
import nl.vu.cs.amstel.msg.MessageRouter;
import nl.vu.cs.amstel.msg.SerializedInboundQueue;
import nl.vu.cs.amstel.user.Combiner;
import nl.vu.cs.amstel.user.MessageValue;
import nl.vu.cs.amstel.user.Vertex;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class Worker<V extends Value, M extends MessageValue> implements AmstelNode<V, M> {

	private static final int LOCAL_INBOX_SIZE = 512;
	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	// for instantiation of user provided classes
	private Class<? extends Vertex<V, M>> vertexClass;
	private MessageFactory<M> messageFactory;
	private VertexValueFactory<V> valuesFactory;
	
	private Ibis ibis;
	private IbisIdentifier master;
	private SendPort masterSender;
	private ReceivePort masterReceiver;
	private ReceivePort receiver;
	private WorkerBarrier barrier;
	private IbisIdentifier[] partitions;
	private InputPartition inputPartition;
	
	// vertex states
	private int crtIndex = -1;
	private Map<Integer, String> idToVertex =
		new HashMap<Integer, String>();
	private boolean[] active;
	
	private Map<String, VertexState<V, M>> vertices = 
		new HashMap<String, VertexState<V, M>>();
	// messages in-boxes
	private InboundQueue<M> inbox;
	private InboundQueue<M> futureInbox;
	
	// messaging entities
	private MessageReceiver<V, M> messageReceiver;
	private MessageRouter<V, M> messageRouter;
	
	// state of the computation
	private WorkerState<M> state;
	
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
	
	private void addVertex(VertexState<V, M> vertex) {
		crtIndex++;
		vertex.setIndex(crtIndex);
		vertices.put(vertex.getID(), vertex);
		idToVertex.put(vertex.getIndex(), vertex.getID());
	}
	
	private void readInput() throws IOException {
		Map<String, String[]> inputVertexes = 
			GraphInput.readVertexes(inputPartition);
		for (String vertex : inputVertexes.keySet()) {
			int value = GraphInput.readValue(vertex);
			V vertexValue = valuesFactory.create(value);
			VertexState<V, M> state = new VertexState<V, M>(vertex, 
				inputVertexes.get(vertex), vertexValue);
			if (messageRouter.getOwner(vertex).equals(ibis.identifier())) {
				// this vertex belongs to me
				addVertex(state);
			} else {
				messageRouter.send(state);				
			}
		}
	}
	
	private void loadReceivedInput() {
		for (VertexState<V, M> vertex : messageReceiver.getReceivedVertexes()) {
			addVertex(vertex);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void initInboxes() {
		int count = crtIndex + 1;
		active = new boolean[count];
		
		if (!messageFactory.hasCombiner()) {
			MessageOutputBuffer<M>[] localInbox =
				new MessageOutputBuffer[count];
			MessageIterator<M> msgIterator = 
				new MessageIterator<M>(messageFactory.create());
			inbox = new SerializedInboundQueue(count, vertices, localInbox,
					msgIterator);
			futureInbox = new SerializedInboundQueue(count, vertices, 
					localInbox, msgIterator);
			for (int i = 0; i < count; i++) {
				active[i] = true;
				localInbox[i] = new MessageOutputBuffer<M>(LOCAL_INBOX_SIZE, 
						idToVertex.get(i));
			}
		} else {
			M[] localInbox = (M[]) Array.newInstance(
					messageFactory.getMessageClass(),
					count);
			inbox = new CombinedInboundQueue(count, messageFactory, vertices,
					localInbox);
			futureInbox = new CombinedInboundQueue(count, messageFactory,
					vertices, localInbox);
			for (int i = 0; i < count; i++) {
				active[i] = true;
			}
		}

		messageRouter.setInbox(inbox);
		state.active = active;
	}
	
	private void setupWorkerConnections() throws IOException {
		receiver = ibis.createReceivePort(Node.W2W_PORT, "worker");
		receiver.enableConnections();
		// this is a thread that only listens for incoming messages
		messageRouter = new MessageRouter<V, M>(ibis, partitions, vertices,
				messageFactory);
		messageReceiver = new MessageReceiver<V, M>(receiver, messageRouter, 
				valuesFactory);
		messageReceiver.start();
		state = new WorkerState<M>(messageRouter);
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
	
	private void computeVertexes(Vertex<V, M> v, MessageIterator<M> msgIterator) 
			throws IOException {
		int msgs = 0;
		for (int i = 0; i < active.length; i++) {
			if (inbox.hasMessages(i) || active[i]) {
				VertexState<V, M> state = vertices.get(idToVertex.get(i));
				v.setState(state);
				// we consider the vertex as active
				active[i] = true;
				v.compute(inbox.getIterator(i));
				// clear all messages
				inbox.clear(i);
			}
			if (inbox.hasMessages(i)) {
				msgs++;
			}
		}
	}
	
	private int countActiveVertexes() {
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
	
	@Override
	public void setCombiner(Class<? extends Combiner<M>> combinerClass) {
		messageFactory.setCombinerClass(combinerClass);
	}
	
	public void run() throws IOException, InterruptedException {
		register();
		setupWorkerConnections();
		// reading input and distribute vertexes
		readInput();
		messageRouter.flush();
		// we synchronize here because we need to be sure that 
		// all the input messages have been received
		barrier.enter(1);
		barrier.enterCooldown();
		loadReceivedInput();
		// initialize vertex arrays
		initInboxes();
		
		state.activeVertexes = vertices.size();
		state.msg = messageFactory.create();
		// instantiate the vertex handler and the message iterator 
		Vertex<V, M> v;
		MessageIterator<M> msgIterator =
			new MessageIterator<M>(messageFactory.create());
		try {
			v = vertexClass.newInstance();
			v.setWorkerState(state);
			// the computation iteration
			while (state.superstep >= 0) {
				messageReceiver.setInbox(futureInbox);
				barrier.enter(state.activeVertexes);				
				// compute phase
				logger.info("Running superstep " + state.superstep);
				computeVertexes(v, msgIterator);				
				// send everything left in the buffers 
				// and wait for acknowledgments
				messageRouter.flush();
				
				// cool-down phase of the super-step
				// in this phase no one is sending/receiving message, so it's
				// safe to switch the message in-boxes 
				state.superstep = barrier.enterCooldown();
				// the current in-box is supposed to be already processed,
				// and it's going to be the futureInbox in the next super-step
				state.activeVertexes = countActiveVertexes();
				swapInboxes();
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		
		// check values
		/*
		int max = 0;
		for (String vertex : vertices.keySet()) {
			int value = vertices.get(vertex).getValue();
			if (value > max) {
				max = value;
			}
		}
		logger.info("Max value found: " + max);
		*/
		
		// close connections
		closeWorkerConnections();
		// exit
		masterSender.close();
		masterReceiver.close();
		ibis.end();
	}
	
	public Worker(Ibis ibis, IbisIdentifier master, 
			Class<? extends Vertex<V, M>> vertexClass,
			Class<V> vertexValueClass,
			Class<M> messageClass)
		throws IOException, InterruptedException {
		// setup
		this.ibis = ibis;
		this.master = master;
		this.vertexClass = vertexClass;
		messageFactory = new MessageFactory<M>(messageClass);
		valuesFactory = new VertexValueFactory<V>(vertexValueClass);
		masterSender = ibis.createSendPort(Node.W2M_PORT);
		masterSender.connect(master, "w2m");
		masterReceiver = ibis.createReceivePort(Node.M2W_PORT, "m2w");
		masterReceiver.enableConnections();
		barrier = new WorkerBarrier(masterSender, masterReceiver);
	}

}
