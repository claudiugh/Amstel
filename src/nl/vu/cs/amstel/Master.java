package nl.vu.cs.amstel;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.graph.GraphInput;
import nl.vu.cs.amstel.graph.InputPartition;
import nl.vu.cs.amstel.user.Combiner;
import nl.vu.cs.amstel.user.MessageValue;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class Master<V, M extends MessageValue> extends AmstelNode<V, M> {

	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel.master");
	
	private Ibis ibis;
	private ReceivePort receiver;
	private SendPort sender;
	private MasterBarrier barrier;
	private IbisIdentifier[] workers;
	
	private void registration() throws Exception {
		logger.info("Begin registration");
		for (int regId = 0; regId < workers.length; regId++) {
			ReadMessage r = receiver.receive();
			String msg = r.readString();
			if (!msg.equals("register")) {
				logger.fatal("'register' expected, got " + msg);
			}
			workers[regId] = r.origin().ibisIdentifier();
			r.finish();
			logger.info(workers[regId] + " joined");
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
	
	private Map<IbisIdentifier, InputPartition> partitionInput() {
		HashMap<IbisIdentifier, InputPartition> partitions = 
			new HashMap<IbisIdentifier, InputPartition>();
		int perWorker = GraphInput.VERTEXES / workers.length;
		int remaining = GraphInput.VERTEXES - workers.length * perWorker;
		int from = 0;
		for (int i = 0; i < workers.length; i++) {
			int count = (i < remaining) ? perWorker + 1 : perWorker;
			partitions.put(workers[i],
				new InputPartition(GraphInput.vertexes[from], count));
			from += count;
		}
		return partitions;
	}
	
	private static String formatTime(long millis) {
		long seconds = millis / 1000;
		if (seconds < 60) {
			return seconds + "s";
		} else {
			long minutes = seconds / 60;
			seconds = seconds % 60;
			return minutes + "m " + seconds + "s";
		}
	}
	
	@Override
	public void setCombiner(Class<? extends Combiner<M>> combinerClass) {
		// the combiner is not useful for the Master node
		// or at least for now...
	}
	
	public void logSuperstepInfo(int activeVertices, int superstep) {
		logger.info("Superstep " + superstep + ": "
				+ activeVertices + " vertexes left");
		for (AggregatorState aggState : aggregators.values()) {
			if (aggState.aggregator.hasValue()) {
				logger.info("Aggregator " + aggState.aggregator.getName() + ": "
						+ aggState.aggregator.getValue());
			}
		}
	}
	
	/**
	 * reset the aggregators unless they are sticky
	 */
	private void manageAggregators() {
		for (AggregatorState aggState : aggregators.values()) {
			if (!aggState.aggregator.isSticky()) {
				aggState.aggregator.reset();
			}
		}	
	}
	
	public void run() throws Exception {
		logger.info("Running for " + GraphInput.VERTEXES + " vertexes"
				+ " and " + GraphInput.EDGES + " edges");
		// record start time
		long startTime = System.currentTimeMillis();
		
		// setup phase 
		receiver.enableConnections();			
		registration();
		// reading input
		barrier.await();
		barrier.release();
		// run the super-steps 
		int superstep = 0;
		int activeVertices = 1;
		while (activeVertices > 0) {
			activeVertices = barrier.awaitAndGetAggregators();
			logSuperstepInfo(activeVertices, superstep);
			if (activeVertices == 0) {
				// end of the algorithm
				superstep = -1;
			}			
			barrier.releaseAndSendAggregators(superstep);
			superstep++;
			manageAggregators();
		}
		// compute running time
		long runningTime = System.currentTimeMillis() - startTime;
		logger.info("Running time: " + formatTime(runningTime));
		// exit
		receiver.close();
		sender.close();
		ibis.end();		
	}

	public Master(Ibis ibis, int workersNo) throws Exception {
		this.ibis = ibis;
		workers = new IbisIdentifier[workersNo];
		receiver = ibis.createReceivePort(Node.W2M_PORT, "w2m");
		sender = ibis.createSendPort(Node.M2W_PORT, "m2w");
		barrier = new MasterBarrier(workersNo, sender, receiver, 
			new AggregatorStream(aggregators));
	}
	
}
