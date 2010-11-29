package nl.vu.cs.amstel.msg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.VertexState;
import nl.vu.cs.amstel.user.MessageValue;

import ibis.ipl.ConnectionClosedException;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class MessageReceiver<M extends MessageValue> extends Thread {

	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	public static final int INPUT_MSG = 0x100;
	public static final int COMPUTE_MSG = 0x200;
	public static final int FLUSH_MSG = 0x300;
	public static final int FLUSH_ACK_MSG = 0x400;
	
	private Class<M> messageClass;
	private Map<String, VertexState<M>> vertexes;
	
	private List<VertexState<M>> inputVertexes = 
		new ArrayList<VertexState<M>>();
	private ReceivePort receiver;
	private MessageRouter<M> router;
	
	public MessageReceiver(ReceivePort receiver, MessageRouter<M> router,
			Class<M> messageClass,
			Map<String, VertexState<M>> vertexes) {
		this.receiver = receiver;
		this.router = router;
		this.messageClass = messageClass;
		this.vertexes = vertexes;
	}
	
	private void inputMessage(ReadMessage msg) throws IOException {
		VertexState<M> vertex = new VertexState<M>();
		vertex.deserialize(msg);
		// stack the vertex to the local list of received vertexes
		inputVertexes.add(vertex);		
		logger.info("Received input vertex " + vertex.getID());
	}
	
	private void computeMessage(ReadMessage r) throws IOException {
		int count = r.readInt();
		for (int i = 0; i < count; i++) {
			String vertex = r.readString();
			int msgCount = r.readInt();
			for (int j = 0; j < msgCount; j++) {
				M msg;
				try {
					msg = messageClass.newInstance();
					msg.deserialize(r);
					// TODO: this will not work
					vertexes.get(vertex).deliver(msg);
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void sendFlushAck(ReadMessage r) throws IOException {
		SendPort sender = router.getSender(r.origin().ibisIdentifier());
		synchronized(sender) {
			WriteMessage w = sender.newMessage();
			w.writeInt(FLUSH_ACK_MSG);
			w.finish();
		}
	}
	
	public List<VertexState<M>> getReceivedVertexes() {
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
				e.printStackTrace();
			}
		}
	}

}
