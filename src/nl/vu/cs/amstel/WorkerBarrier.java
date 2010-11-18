package nl.vu.cs.amstel;

import java.io.IOException;

import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class WorkerBarrier {

	private SendPort sender;
	private ReceivePort receiver;
	
	public WorkerBarrier(SendPort sender, ReceivePort receiver) {
		this.sender = sender;
		this.receiver = receiver;
	}
	
	/**
	 * blocks until every reached the barrier and the master ordered the release
	 * it sends the number of active vertexes
	 * @throws IOException
	 * @return true if the algorithm is finished
	 *         false otherwise
	 */
	public int enter(int activeVertexes) throws IOException {
		// enqueue myself in the barrier
		WriteMessage w = sender.newMessage();
		w.writeInt(MasterBarrier.BARRIER_ENTER);
		w.writeInt(activeVertexes);
		w.finish();
		// block until I get the release message
		ReadMessage r = receiver.receive();
		int code = r.readInt();
		if (code != MasterBarrier.BARRIER_RELEASE) {
			System.err.println("Incorrect barrier release code. Expecting "
					+ MasterBarrier.BARRIER_RELEASE + ", got " + code);
		}
		int superstep = r.readInt();
		r.finish();
		return superstep;
	}
	
}
