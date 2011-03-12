package nl.vu.cs.amstel.examples;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import nl.vu.cs.amstel.user.MessageValue;

public class SemiClusterMessage implements MessageValue {

	public SemiClusterList clustersList = new SemiClusterList();
	
	@Override
	public <M extends MessageValue> void copy(M other) {
		SemiClusterMessage m = (SemiClusterMessage) other;
		clustersList.clusters = m.clustersList.clusters;
	}

	@Override
	public void deserialize(DataInputStream in) throws IOException {
		clustersList.deserialize(in);
	}

	@Override
	public void serialize(DataOutputStream out) throws IOException {
		clustersList.serialize(out);
	}

}
