package nl.vu.cs.amstel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.graph.GraphInput;
import nl.vu.cs.amstel.graph.InputPartition;
import nl.vu.cs.amstel.msg.MessageFactory;
import nl.vu.cs.amstel.msg.MessageIterator;
import nl.vu.cs.amstel.msg.MessageReceiver;
import nl.vu.cs.amstel.msg.MessageRouter;
import nl.vu.cs.amstel.user.MessageValue;
import nl.vu.cs.amstel.user.Vertex;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class Worker<M extends MessageValue> {

	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	// for instantiation of user provided classes
	private Class<? extends Vertex<M>> vertexClass;
	private MessageFactory<M> messageFactory;

	private Ibis ibis;
	private IbisIdentifier master;
	private SendPort masterSender;
	private ReceivePort masterReceiver;
	private ReceivePort receiver;
	private WorkerBarrier barrier;
	private IbisIdentifier[] partitions;
	private InputPartition inputPartition;
	
	// vertex states
	private Map<String, VertexState<M>> vertexes = 
		new HashMap<String, VertexState<M>>();
	// messages in-boxes
	private Map<String, List<byte[]>> inbox =
		new HashMap<String, List<byte[]>>();
	private Map<String, List<byte[]>> futureInbox = 
		new HashMap<String, List<byte[]>>();
	
	// messaging entities
	private MessageReceiver<M> messageReceiver;
	private MessageRouter<M> messageRouter;
	
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
	
	private void addVertex(VertexState<M> vertex) {
		vertexes.put(vertex.getID(), vertex);
		inbox.put(vertex.getID(), new ArrayList<byte[]>());
		futureInbox.put(vertex.getID(), new ArrayList<byte[]>());
	}
	
	private void readInput() throws IOException {
		Map<String, String[]> inputVertexes = 
			GraphInput.readVertexes(inputPartition);
		for (String vertex : inputVertexes.keySet()) {
			VertexState<M> state = new VertexState<M>(vertex, 
				inputVertexes.get(vertex), GraphInput.readValue(vertex));
			if (messageRouter.getOwner(vertex).equals(ibis.identifier())) {
				// this vertex belongs to me
				addVertex(state);
			} else {
				messageRouter.send(state);				
			}
		}
	}
	
	private void loadReceivedInput() {
		for (VertexState<M> vertex : messageReceiver.getReceivedVertexes()) {
			addVertex(vertex);
		}
	}
	
	private void setupWorkerConnections() throws IOException {
		receiver = ibis.createReceivePort(Node.W2W_PORT, "worker");
		receiver.enableConnections();
		// this is a thread that only listens for incoming messages
		messageRouter = new MessageRouter<M>(ibis, partitions, vertexes);
		messageReceiver = new MessageReceiver<M>(receiver, messageRouter);
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
	
	private void computeVertexes(Vertex<M> v, MessageIterator<M> msgIterator) 
			throws IOException {
		for (String vertex : vertexes.keySet()) {
			List<byte[]> msgInbox = inbox.get(vertex);
			VertexState<M> state = vertexes.get(vertex);
			if (state.hasLocalMessages()) {
				msgInbox.add(state.getLocalBuffer());
			}
			if (msgInbox.size() > 0 || state.isActive()) {
				v.setState(state);
				// we consider the vertex as active
				msgIterator.setBuffers(msgInbox);
				v.compute(msgIterator);
				// clear all messages
				msgInbox.clear();
			}
		}
	}
	
	private int countActiveVertexes() {
		int count = 0;
		for (String vertex : vertexes.keySet()) {
			if (futureInbox.get(vertex).size() > 0 ||
					vertexes.get(vertex).isActive()) {
				count++;
			}
		}
		return count;
	}
	
	private void swapInboxes() {
		Map<String, List<byte[]>> tmp = inbox;
		inbox = futureInbox;
		futureInbox = tmp;
	}
	
	private void run() throws IOException, InterruptedException {
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
		
		state.activeVertexes = vertexes.size();
		// instantiate the vertex handler and the message iterator 
		Vertex<M> v;
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
		int max = 0;
		for (String vertex : vertexes.keySet()) {
			int value = vertexes.get(vertex).getValue();
			if (value > max) {
				max = value;
			}
		}
		logger.info("Max value found: " + max);
		
		// close connections
		closeWorkerConnections();
	}
	
	public Worker(Ibis ibis, IbisIdentifier master, 
			Class<? extends Vertex<M>> vertexClass,
			Class<M> messageClass)
		throws IOException, InterruptedException {
		// setup
		this.ibis = ibis;
		this.master = master;
		this.vertexClass = vertexClass;
		messageFactory = new MessageFactory<M>(messageClass);
		masterSender = ibis.createSendPort(Node.W2M_PORT);
		masterSender.connect(master, "w2m");
		masterReceiver = ibis.createReceivePort(Node.M2W_PORT, "m2w");
		masterReceiver.enableConnections();
		barrier = new WorkerBarrier(masterSender, masterReceiver);
		
		// run the work
		run();
		
		// exit
		masterSender.close();
		masterReceiver.close();
		ibis.end();
	}
}
