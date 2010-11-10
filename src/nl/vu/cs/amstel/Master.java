package nl.vu.cs.amstel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.amstel.graph.GraphInput;
import nl.vu.cs.amstel.graph.InputPartition;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class Master {

	private Ibis ibis;
	private ReceivePort receiver;
	private SendPort sender;
	private IbisIdentifier[] workers;
	
	private void barrierWait() throws IOException {
		System.out.println("Waiting for everyone...");
		for (int i = 0; i < workers.length; i++) {
			ReadMessage r = receiver.receive();
			String message = r.readString();
			if (!message.equals("barrier")) {
				System.err.println("expecting 'barrier', got " + message);
			}
			r.finish();
		}
	}
	
	private void barrierRelease() throws IOException {
		WriteMessage w = sender.newMessage();
		w.writeString("release");
		w.finish();
	}
	
	private void registration() throws Exception {
		System.out.println("Begin registration");
		for (int regId = 0; regId < workers.length; regId++) {
			ReadMessage r = receiver.receive();
			String msg = r.readString();
			if (!msg.equals("register")) {
				System.err.println("'register' expected, got " + msg);
			}
			workers[regId] = r.origin().ibisIdentifier();
			r.finish();
			System.out.println("Received from " + workers[regId]);
		}
		
		for (int i = 0; i < workers.length; i++) {
			sender.connect(workers[i], "m2w");
		}
		// we have to send the partitions distribution to each worker
		// in this case partitions are 1-1 mapped to workers
		WriteMessage w = sender.newMessage();
		// send the partitions 
		w.writeObject(workers);
		// send the input partitions
		w.writeObject(partitionInput());
		w.finish();
	}
	
	private void superstep() throws IOException {
		barrierWait();
		System.out.println("perform superstep");
		barrierRelease();
	}
	
	private void run() throws Exception {
		registration();
		for (int i = 0; i < 2; i++) {
			superstep();
		}
	}
	
	private Map<IbisIdentifier, InputPartition> partitionInput() {
		HashMap<IbisIdentifier, InputPartition> partitions = 
			new HashMap<IbisIdentifier, InputPartition>();
		int perWorker = GraphInput.VERTEXES / workers.length;
		int remaining = GraphInput.VERTEXES - workers.length * perWorker;
		int from = 0;
		for (int i = 0; i < workers.length; i++) {
			int count = (i < remaining) ? perWorker + 1 : perWorker;
			partitions.put(workers[i],
				new InputPartition(GraphInput.vertexes[from], count)
			);
			from += count;
		}
		System.out.println(partitions);
		return partitions;
	}
	
	public Master(Ibis ibis, int workersNo) throws Exception {
		this.ibis = ibis;
		workers = new IbisIdentifier[workersNo];
		receiver = ibis.createReceivePort(Node.W2M_PORT, "w2m");
		receiver.enableConnections();
		sender = ibis.createSendPort(Node.M2W_PORT, "m2w");
		run();
		receiver.close();
		sender.close();
		ibis.end();
	}
	
}
