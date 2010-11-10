package nl.vu.cs.amstel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import nl.vu.cs.amstel.graph.GraphInput;
import nl.vu.cs.amstel.graph.InputPartition;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class Worker {

	private Ibis ibis;
	private IbisIdentifier master;
	private SendPort masterSender;
	private ReceivePort masterReceiver;
	private WorkerBarrier barrier;
	private IbisIdentifier[] partitions;
	private InputPartition inputPartition;
	
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
			System.out.println("My input partition is: " + inputPartition);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		r.finish();
	}
	
	private void readInput() throws IOException {
		Map<String, ArrayList<String>> inputVertexes = 
			GraphInput.readVertexes(inputPartition);
		System.out.println("Input data: " + inputVertexes);
	}
	
	private void run() throws IOException {
		register();
		readInput();
		barrier.enter();
		System.out.println("Step 1");
		barrier.enter();
		System.out.println("Step 2: Exit");
	}
	
	public Worker(Ibis ibis, IbisIdentifier master) throws IOException {
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
