package nl.vu.cs.amstel.examples;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.user.Value;

public class SemiClusterList implements Value {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");

	public SemiCluster[] clusters;
	
	public SemiClusterList() {
		clusters = new SemiCluster[0];
	}
	
	public SemiClusterList(String value) {
		clusters = new SemiCluster[0];
	}
	
	public SemiCluster getLast() {
		if (clusters.length > 0) {
			return clusters[clusters.length - 1];
		}
		return null;
	}
	
	@Override
	public void deserialize(DataInputStream in) throws IOException {
		int clustersNo = in.readInt();
		clusters = new SemiCluster[clustersNo];
		for (int i = 0; i < clustersNo; i++) {
			clusters[i] = new SemiCluster();
			clusters[i].deserialize(in);
		}
	}

	@Override
	public void serialize(DataOutputStream out) throws IOException {
		out.writeInt(clusters.length);
		for (SemiCluster cluster : clusters) {
			cluster.serialize(out);
		}
	}

	/**
	 * used only in testing
	 * @return
	 */
	public boolean hasDuplicates() {
		for (int i = 0; i < clusters.length - 1; i++) {
			if (clusters[i].compareTo(clusters[i + 1]) == 0) {
				return true;
			}
		}
		return false;
	}
	
	public String toString() {
		String str = "\n";
		for (SemiCluster cluster : clusters) {
			str += cluster.toString() + "\n";
		}
		return str;
	}
}
