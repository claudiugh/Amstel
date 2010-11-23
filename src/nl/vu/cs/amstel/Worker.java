package nl.vu.cs.amstel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.graph.GraphInput;
import nl.vu.cs.amstel.graph.InputPartition;
import nl.vu.cs.amstel.msg.MessageReceiver;
import nl.vu.cs.amstel.msg.MessageRouter;
import nl.vu.cs.amstel.user.Vertex;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class Worker {

	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private Ibis ibis;
	private IbisIdentifier master;
	private SendPort masterSender;
	private ReceivePort masterReceiver;
	private ReceivePort receiver;
	private WorkerBarrier barrier;
	private IbisIdentifier[] partitions;
	private InputPartition inputPartition;
	private Map<String, VertexState> vertexes = 
		Collections.synchronizedMap(new HashMap<String, VertexState>()); 
	
	// messaging entities
	private MessageReceiver messageReceiver;
	private MessageRouter messageRouter;
	
	// state of the computation
	private WorkerState state;
	
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
	
	private void readInput() throws IOException {
		Map<String, ArrayList<String>> inputVertexes = 
			GraphInput.readVertexes(inputPartition);
		for (String vertex : inputVertexes.keySet()) {
			VertexState state = new VertexState(vertex, 
				inputVertexes.get(vertex), 10);
			messageRouter.send(state);
		}
	}
	
	private void setupWorkerConnections() throws IOException {
		receiver = ibis.createReceivePort(Node.W2W_PORT, "worker");
		receiver.enableConnections();
		// this is a thread that only listens for incoming messages
		messageRouter = new MessageRouter(ibis, partitions, vertexes);
		messageReceiver = new MessageReceiver(receiver, messageRouter, 
			vertexes);
		messageReceiver.start();
		state = new WorkerState(messageRouter);
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
	
	private void computeVertexes(Vertex v) throws IOException {
		for (String vertex : vertexes.keySet()) {
			VertexState state = vertexes.get(vertex);
			if (state.isActive()) {
				v.setState(state);
				state.nextSuperstep();
				v.compute(state.getInbox());
			}
		}
	}
	
	private int nextSuperstep() {
		int activeVertexes = 0;
		for (String vertex : vertexes.keySet()) {
			if (vertexes.get(vertex).nextSuperstep()) {
				activeVertexes++;
			}
		}
		return activeVertexes;
	}
	
	private void run() throws IOException, InterruptedException {
		register();
		setupWorkerConnections();
		// reading input and distribute vertexes
		readInput();
		messageRouter.flush();
		barrier.enter(1);
		// hack: lets time to deliver the messages
		state.activeVertexes = vertexes.size();
		Vertex v = new Vertex();
		v.setWorkerState(state);
		while ((state.superstep = barrier.enter(state.activeVertexes)) >= 0) {
			if (state.superstep == 0) {
				logger.info("Data: " + vertexes);
			}
			logger.info("Running superstep " + state.superstep);
			computeVertexes(v);
			messageRouter.flush();
			state.activeVertexes = nextSuperstep();
		}
		
		// close connections
		closeWorkerConnections();
	}
	
	public Worker(Ibis ibis, IbisIdentifier master) throws IOException, InterruptedException {
		// setup
		this.ibis = ibis;
		this.master = master;
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
