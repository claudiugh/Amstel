package nl.vu.cs.amstel.msg;

import java.io.IOException;
import java.util.Map;

import nl.vu.cs.amstel.VertexState;
import nl.vu.cs.amstel.user.MessageValue;

import ibis.ipl.ConnectionClosedException;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.SendPort;
import ibis.ipl.WriteMessage;

public class MessageReceiver extends Thread {

	public static final int INPUT_MSG = 0x100;
	public static final int COMPUTE_MSG = 0x200;
	public static final int FLUSH_MSG = 0x300;
	public static final int FLUSH_ACK_MSG = 0x400;
	
	private Map<String, VertexState> vertexes;
	private ReceivePort receiver;
	private MessageRouter router;
	
	public MessageReceiver(ReceivePort receiver, MessageRouter router,
			Map<String, VertexState> vertexes) {
		this.receiver = receiver;
		this.router = router;
		this.vertexes = vertexes;
	}
	
	private void inputMessage(ReadMessage msg) throws IOException {
		VertexState vertex = VertexState.deserialize(msg);
		vertexes.put(vertex.getID(), vertex);		
		System.out.println("Received input vertex " + vertex.getID());
	}
	
	private void computeMessage(ReadMessage r) throws IOException {
		int count = r.readInt();
		for (int i = 0; i < count; i++) {
			String vertex = r.readString();
			int msgCount = r.readInt();
			for (int j = 0; j < msgCount; j++) {
				MessageValue msg = new MessageValue();
				msg.deserialize(r);
				vertexes.get(vertex).deliver(msg);
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
				default: System.err.println("Unknown message type");
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
