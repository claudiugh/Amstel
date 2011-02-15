package nl.vu.cs.amstel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.user.Aggregator;

/**
 * Class used for packing and unpacking aggregated values over the network.
 * It is ought to be used in both master and worker nodes similarly, with
 * and exception at unpacking, where the actions for the received values is
 * different.
 * 
 * @author claudiugh
 *
 */
public class AggregatorStream {

	protected Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private static final int BUFFER_SIZE = 128; 
	
	private Map<String, AggregatorState> aggregators;
	private ByteArrayOutputStream aggSendBuffer = 
		new ByteArrayOutputStream(BUFFER_SIZE);	
	private DataOutputStream aggStream = new DataOutputStream(aggSendBuffer);
	
	public AggregatorStream(Map<String, AggregatorState> aggregators) {
		this.aggregators = aggregators;
	}
	
	public byte[] pack() throws IOException {
		aggSendBuffer.reset();
		for (AggregatorState aggState : aggregators.values()) {
			if (aggState.aggregator.hasValue()) {
				aggStream.writeUTF(aggState.aggregator.getName());
				aggState.aggregator.getValue().serialize(aggStream);
			}
		}
		return aggSendBuffer.toByteArray();
	}
	
	@SuppressWarnings("unchecked")
	private void unpack(byte[] buffer, boolean combine) throws IOException {
		ByteArrayInputStream aggByteStream = new ByteArrayInputStream(buffer);
		DataInputStream aggDataStream = new DataInputStream(aggByteStream);
		while (aggByteStream.available() > 0) {
			String aggKey = aggDataStream.readUTF();
			if (!aggregators.containsKey(aggKey)) {
				logger.error("Unknown aggregator: " + aggKey);
				break;
			}
			AggregatorState aggState = aggregators.get(aggKey);
			// using the current value to deserialize value
			Aggregator agg = aggState.aggregator;
			if (aggState.currentValue == null) {
				aggState.currentValue = agg.newValue();
			}
			aggState.currentValue.deserialize(aggDataStream);
			if (combine) {
				if (agg.hasValue()) {
					agg.combine(aggState.currentValue);
				} else {
					agg.init(aggState.currentValue);
				}
			}
		}		
	}
	
	/**
	 * unpack for worker node
	 */
	public void unpackAndUpdate(byte[] buffer) throws IOException {
		unpack(buffer, false);
	}
	
	/**
	 * unpack for master node
	 * @param buffer
	 * @throws IOException
	 */
	public void unpackAndCombine(byte[] buffer) throws IOException {
		unpack(buffer, true);
	}
}
