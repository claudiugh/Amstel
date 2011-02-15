package nl.vu.cs.amstel.msg;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.graph.VertexFactory;
import nl.vu.cs.amstel.graph.VertexState;
import nl.vu.cs.amstel.user.MessageValue;
import nl.vu.cs.amstel.user.Value;

import ibis.ipl.ConnectionClosedException;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class MessageReceiver<V extends Value, E extends Value, 
		M extends MessageValue> extends Thread {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	public static final int INPUT_MSG = 0x100;
	public static final int COMPUTE_MSG = 0x200;
	public static final int FLUSH_MSG = 0x300;
	public static final int FLUSH_ACK_MSG = 0x400;
	
	protected ReceivePort receiver;
	protected MessageRouter<V, E, M> router;
	protected VertexFactory<V, E> valuesFactory;
	
	private InboundQueue<M> inbox = null;
	private List<VertexState<V, E, M>> inputVertexes = 
		new ArrayList<VertexState<V, E, M>>();
	
	public MessageReceiver(ReceivePort receiver, MessageRouter<V, E, M> router,
			VertexFactory<V, E> valuesFactory) {
		this.receiver = receiver;
		this.router = router;
		this.valuesFactory = valuesFactory;
	}

	private void inputMessage(ReadMessage msg) throws IOException {
		VertexState<V, E, M> vertex = new VertexState<V, E, M>();
		int bufferSize = msg.readInt();
		byte[] buffer = new byte[bufferSize];
		msg.readArray(buffer);
		DataInputStream inStream = new DataInputStream(
				new ByteArrayInputStream(buffer));
		vertex.deserialize(inStream, valuesFactory);
		// stack the vertex to the local list of received vertexes
		inputVertexes.add(vertex);
	}
	
	private void computeMessage(ReadMessage r) throws IOException {
		inbox.deliver(r);
	}

	private void sendFlushAck(ReadMessage r) throws IOException {
		SendPort sender = router.getSender(r.origin().ibisIdentifier());
		synchronized(sender) {
			WriteMessage w = sender.newMessage();
			w.writeInt(FLUSH_ACK_MSG);
			w.finish();
		}
	}
	
	public void setInbox(InboundQueue<M> inbox) {
		this.inbox = inbox;
	}
	
	public List<VertexState<V, E, M>> getReceivedVertexes() {
		return inputVertexes;
	}
	
	public void run() {
		while (true) {
			try {
				ReadMessage r = receiver.receive();
				// the first Int is the message type
				int msgType = r.readInt();
				switch (msgType) {
				case INPUT_MSG: inputMessage(r); break;
				case COMPUTE_MSG: computeMessage(r); break;
				case FLUSH_MSG: sendFlushAck(r); break;
				case FLUSH_ACK_MSG: 
					router.deactivateWorker(r.origin().ibisIdentifier()); 
					break;
				default: logger.error("Unknown message type");
				}
				r.finish();
			} catch (ConnectionClosedException e) {
				// this is caused by receiver.close() call from the main thread
				break;	
			} catch (IOException e) {
				logger.fatal("Error in Message Receiver", e);
			}
		}
	}

}
