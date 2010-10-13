package nl.vu.cs.amstel;

import java.util.concurrent.CyclicBarrier;

public class Master extends Thread {

	public CyclicBarrier inputBarrier = null;
	public CyclicBarrier barrier = null;
	public Worker[] workers = null;
	private int partitions;
	
	private boolean done = false;
	private int superstep = 0;
	
	public Master(int partitions, Class<? extends Vertex<?>> vertexClass) {
		super();
		this.partitions = partitions;
		setup(vertexClass);
	}
	
	private void setup(Class<? extends Vertex<?>> vertexClass) {
		// we have one worker for each partition
		int workersNo = this.partitions;
		this.inputBarrier = new CyclicBarrier(workersNo);
		// this barrier is for the BSP model
		this.barrier = new CyclicBarrier(workersNo, new Runnable() {
			public void run() {
				System.out.println("Superstep " + superstep + " is over");
				done = checkStopCondition();
				setSuperstep(superstep + 1);
			}
		});
		workers = new Worker[workersNo];
		int perworker = GraphInput.graph.length / workersNo;
		for (int i = 0; i < workers.length; i++) {
			workers[i] = new Worker(i, this, vertexClass);
			workers[i].setInput(i * perworker, perworker);
		}		
	}
	
	private boolean checkStopCondition() {
		for (int i = 0; i < workers.length; i++) {
			if (workers[i].getActiveVertexes() > 0 || 
				workers[i].mailbox.size() > 0) {
				return false;
			}
		}
		return true;
	}
	
	private void setSuperstep(int superstep) {
		this.superstep = superstep;
		for (int i = 0; i < workers.length; i++) {
			workers[i].setSuperstep(superstep);
		}
	}
	
	public int getPartition(int vertexID) {
		return vertexID % partitions;
	}
		
	private void joinAll() {
		for (int i = 0; i < workers.length; i++) {
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}		
	}
	
	public boolean isDone() {
		return done;
	}
	
	public void run() {
		System.out.println("Master " + getId());
		// start workers
		for (int i = 0; i < workers.length; i++) {
			workers[i].start();
		}
		
		joinAll();
	}
}
