package nl.vu.cs.amstel;

import java.io.IOException;

import org.apache.log4j.Logger;

import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class WorkerBarrier {

	private Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private SendPort sender;
	private ReceivePort receiver;
	
	public WorkerBarrier(SendPort sender, ReceivePort receiver) {
		this.sender = sender;
		this.receiver = receiver;
	}
	
	protected void waitRelease(ReadMessage r) throws IOException {
		int code = r.readInt();
		if (code != MasterBarrier.BARRIER_RELEASE) {
			logger.fatal("Incorrect barrier release code. Expecting "
				+ MasterBarrier.BARRIER_RELEASE + ", got " + code);
		}		
	}
	
	/**
	 * enter in the cool-down phase of the super-step
	 * call order:
	 * 	 	1. enter()
	 * 		2. enterCooldown()
	 * WARNING: NEVER enter in the barrier in a different order 
	 * @throws IOException
	 * @return the number of the super-step
	 * 		   -1 if the algorithm is finished
	 */
	public int enterCooldown() throws IOException {
		WriteMessage w = sender.newMessage();
		w.writeInt(MasterBarrier.BARRIER_ENTER_COOLDOWN);
		w.finish();
		// wait for release
		ReadMessage r = receiver.receive();
		waitRelease(r);
		int superstep = r.readInt();
		r.finish();
		return superstep;
	}
	
	/**
	 * blocks until every reached the barrier and the master ordered the release
	 * it sends the number of active vertexes
	 * @throws IOException
	 */
	public void enter(int activeVertexes) throws IOException {
		// enqueue myself in the barrier
		WriteMessage w = sender.newMessage();
		w.writeInt(MasterBarrier.BARRIER_ENTER);
		w.writeInt(activeVertexes);
		w.finish();
		// block until I get the release message
		ReadMessage r = receiver.receive();
		waitRelease(r);
		r.finish();
	}
	
}
