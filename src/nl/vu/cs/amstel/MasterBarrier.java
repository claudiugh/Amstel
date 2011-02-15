package nl.vu.cs.amstel;

import java.io.IOException;

import org.apache.log4j.Logger;

import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class MasterBarrier {

	protected Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	public static final int BARRIER_ENTER = 1;
	public static final int BARRIER_ENTER_COOLDOWN = 2;
	public static final int BARRIER_RELEASE = 3;
	
	private int members;
	private SendPort sender;
	private ReceivePort receiver;
	private AggregatorStream aggStream;
	
	public MasterBarrier(int members, SendPort sender, ReceivePort receiver,
			AggregatorStream aggStream) {
		this.members = members;
		this.sender = sender;
		this.receiver = receiver;
		this.aggStream = aggStream;
	}
	
	protected void awaitCooldown() throws IOException {
		for (int i = 0; i < members; i++) {
			ReadMessage r = receiver.receive();
			int code = r.readInt();
			if (code != BARRIER_ENTER_COOLDOWN) {
				logger.fatal("Incorrect barrier enter code. "
					+ "Expecting " + BARRIER_ENTER + ", got " + code);
			}
			r.finish();
		}
	}
	
	/**
	 * blocks until all members enter in the barrier
	 * @throws IOException 
	 */
	public int await() throws IOException {
		int activeVertexes = 0;
		for (int i = 0; i < members; i++) {
			ReadMessage r = receiver.receive();
			int code = r.readInt();
			if (code != BARRIER_ENTER) {
				logger.fatal("Incorrect barrier enter code. "
					+ "Expecting " + BARRIER_ENTER + ", got " + code);
			}
			activeVertexes += r.readInt();
			// unpack aggregators
			int bufferSize = r.readInt();
			byte[] buffer = new byte[bufferSize];
			r.readArray(buffer);
			aggStream.unpackAndCombine(buffer);
			r.finish();
		}
		WriteMessage w = sender.newMessage();
		w.writeInt(BARRIER_RELEASE);
		// send aggregators
		byte[] aggBuffer = aggStream.pack();
		w.writeInt(aggBuffer.length);
		w.writeArray(aggBuffer);
		w.finish();
		
		// cool-down phase
		awaitCooldown();
		
		// use the number of active vertexes to decide the state of the next
		// super-step
		return activeVertexes;
	}
	
	/**
	 * Releases all members from the barrier
	 * @throws IOException
	 */
	public void release(int superstep) throws IOException {
		WriteMessage w = sender.newMessage();
		w.writeInt(BARRIER_RELEASE);
		w.writeInt(superstep);
		w.finish();
	}
}
