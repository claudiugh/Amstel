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
	
	private void awaitCooldown() throws IOException {
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
	 * general purpose synchronization
	 * @throws IOException 
	 */
	public void await() throws IOException {
		for (int i = 0; i < members; i++) {
			ReadMessage r = receiver.receive();
			int code = r.readInt();
			if (code != BARRIER_ENTER) {
				logger.fatal("Incorrect barrier enter code. "
					+ "Expecting " + BARRIER_ENTER + ", got " + code);
			}
			r.finish();
		}		
	}
	
	public void release() throws IOException {
		WriteMessage w = sender.newMessage();
		w.writeInt(BARRIER_RELEASE);
		w.finish();
	}
	
	/**
	 * blocks until all members enter in the barrier
	 * @throws IOException 
	 */
	public int awaitAndGetAggregators() throws IOException {
		int activeVertices = 0;
		for (int i = 0; i < members; i++) {
			ReadMessage r = receiver.receive();
			int code = r.readInt();
			if (code != BARRIER_ENTER) {
				logger.fatal("Incorrect barrier enter code. "
					+ "Expecting " + BARRIER_ENTER + ", got " + code);
			}
			activeVertices += r.readInt();
			// unpack aggregators
			int bufferSize = r.readInt();
			byte[] buffer = new byte[bufferSize];
			r.readArray(buffer);
			aggStream.unpackAndCombine(buffer);
			r.finish();
		}
		return activeVertices;
	}
	
	/**
	 * This is a two-step release. Firstly it sends all the data (superstep,
	 * aggregators) with the release. After that enters in the cool-down stage, releasing
	 * for the second time.
	 * 
	 * @throws IOException
	 */
	public void releaseAndSendAggregators(int superstep) throws IOException {
		WriteMessage w = sender.newMessage();
		w.writeInt(BARRIER_RELEASE);
		w.writeInt(superstep);
		// send aggregators
		byte[] aggBuffer = aggStream.pack();
		w.writeInt(aggBuffer.length);
		w.writeArray(aggBuffer);
		w.finish();
		
		// cool-down phase
		awaitCooldown();
		
		// release from cool-down
		release();
	}
}
