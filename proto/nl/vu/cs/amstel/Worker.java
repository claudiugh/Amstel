package nl.vu.cs.amstel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

interface MessageRouter {
	public void send(int dst, Message<?> msg);
}

public class Worker extends Thread {

	public List<Object> mailbox = 
		Collections.synchronizedList(new ArrayList<Object>());
	
	private Class<? extends Vertex<?>> vertexClass;
	private int id;
	private Master master;	
	private int superstep = 0;
	private int inputOffset = 0;
	private int inputLen = 0;
	
	public MessageRouter msgRouter;
	public Set<Integer> activeVertexes = 
		Collections.synchronizedSet(new HashSet<Integer>());
	private HashMap<Integer, Vertex> vertexes = 
		new HashMap<Integer, Vertex>();
	private HashMap<Integer, VertexState> states =
		new HashMap<Integer, VertexState>();
	private HashMap<Integer, List<Message>> messages = 
		new HashMap<Integer, List<Message>>();
	
	private void tryEnterBarrier(CyclicBarrier barrier) {
		try {
			barrier.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
		}		
	}
	
	private void addVertex(int vertexId, int value, List<Integer> edgesList) {
		VertexState state = new VertexState(vertexId, this, value, edgesList);
		Vertex v = null;
		// instantiation
		try {
			v = vertexClass.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		v.setState(state);
		states.put(vertexId, state);
		vertexes.put(vertexId, v);
		messages.put(vertexId, new ArrayList<Message>());
	}
	
	private void setup() {
		this.msgRouter = new MessageRouter() {
			public void send(int dst, Message<?> msg) {
				int responsible = master.getPartition(dst);
				master.workers[responsible].mailbox.add(msg);
			}
		};
	}
	
	public Worker(int id, Master master, Class<? extends Vertex<?>> vertexClass) {
		super();
		this.id = id;
		this.master = master;
		this.vertexClass = vertexClass;
		setup();
	}
	
	public void setInput(int offset, int len) {
		this.inputOffset = offset;
		this.inputLen = len;
	}
	
	public void setSuperstep(int superstep) { 
		this.superstep = superstep;
	}
	
	public int getSuperstep() {
		return superstep;
	}
	
	public int getActiveVertexes() {
		return activeVertexes.size();
	}
	
	public void readInput() {
		for (int i = inputOffset; i < inputOffset + inputLen; i++) {
			int vertexId = i;
			ArrayList<Integer> vdata = GraphInput.graph[vertexId];
			int value = vdata.get(0);
			List<Integer> edgesList = vdata.subList(1, vdata.size());
			int responsible = master.getPartition(vertexId);
			if (responsible == id) {
				addVertex(vertexId, value, edgesList);
			} else {
				Object message = new InputMessage(vertexId, value, edgesList);
				master.workers[responsible].mailbox.add(message);
			}
		}
		// wait for every worker to finish reading input
		tryEnterBarrier(master.inputBarrier);
		// get the messages as input messages
		for (Object message : mailbox) {
			InputMessage m = (InputMessage)message;
			addVertex(m.vertexId, m.value, m.edges);
		}
		mailbox.clear();
		activeVertexes.addAll(vertexes.keySet());
		tryEnterBarrier(master.inputBarrier);
	}
	
	private void readInbox() {
		synchronized(mailbox) {
			for (Object m : mailbox) {
				Message msg = (Message)m;
				messages.get(msg.dstVid).add(msg);
				states.get(msg.dstVid).setActive(true);
			}
			mailbox.clear();
		}
	}
	
	private void computeVertexes() {
		for (Integer vid : vertexes.keySet()) {
			Vertex v = vertexes.get(vid);
			v.compute(messages.get(vid));
		}
	}
	
	public void run() {
		readInput();
		do {
			readInbox();
			computeVertexes();
			tryEnterBarrier(master.barrier);
		} while (!master.isDone());
	}
}
