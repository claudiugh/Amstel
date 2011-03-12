package nl.vu.cs.amstel.examples;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import nl.vu.cs.amstel.VertexIdStorage;
import nl.vu.cs.amstel.user.Value;

public class SemiCluster implements Value, Comparable<SemiCluster> {

	public final double BOUNDARY_EDGE_FACTOR = 0.5;
	
	public int internalWeight = 0;
	public int boundaryWeight = 0;
	public String[] members;
	
	private double score = 0.0;
	
	/**
	 * this constructor will be called before deserialization
	 */
	public SemiCluster() {
	}
	
	public SemiCluster(String vertex, int outCost) {
		members = new String[] {vertex};
		internalWeight = 0;
		boundaryWeight = outCost;
		//score = -outCost * BOUNDARY_EDGE_FACTOR;
		score = -100.0;
	}
	
	private double computeScore() {
		int cliqueSize = (members.length * (members.length - 1)) / 2;
		return 
			((double)internalWeight - BOUNDARY_EDGE_FACTOR * boundaryWeight) /
			(double) cliqueSize;
	}
	
	public SemiCluster(String[] vertices, int internalWeight, 
			int boundaryWeight) {
		this.members = vertices;
		this.internalWeight = internalWeight;
		this.boundaryWeight = boundaryWeight;
		score = computeScore();
		Arrays.sort(members);
	}
	
	public boolean equals(SemiCluster other) {
		if (size() != other.size()) {
			return false;
		}
		for (int i = 0; i < members.length; i++) {
			if (!members[i].equals(other.members[i])) {
				return false;
			}
		}
		return true;
	}
	
	public boolean contains(String vertex) {
		return Arrays.binarySearch(members, vertex) >= 0;
	}
	
	public int size() {
		return members.length;
	}
	
	public double getScore() {
		return score;
	}

	@Override
	public void deserialize(DataInputStream in) throws IOException {
		internalWeight = in.readInt();
		boundaryWeight = in.readInt();
		score = in.readDouble();
		int membersCount = in.readInt();
		members = new String[membersCount];
		for (int i = 0; i < membersCount; i++) {
			members[i] = VertexIdStorage.get(in.readUTF());
		}
	}

	@Override
	public void serialize(DataOutputStream out) throws IOException {
		out.writeInt(internalWeight);
		out.writeInt(boundaryWeight);
		out.writeDouble(score);
		out.writeInt(members.length);
		for (String vertex : members) {
			out.writeUTF(vertex);
		}
	}

	/**
	 * reversed order
	 */
	@Override
	public int compareTo(SemiCluster o) {
		if (score < o.score) {
			return 1;
		} else if (score > o.score) {
			return -1;
		}
		if (members.length != o.members.length) {
			return members.length - o.members.length;
		}
		// they have the same number of members
		for (int i = 0; i < members.length; i++) {
			int cmp = members[i].compareTo(o.members[i]);
			if (cmp != 0) {
				return cmp;
			}
		}
		return 0;
	}
	
	public String toString() {
		String str = "[";
		for (int i = 0; i < members.length - 1; i++) {
			str += members[i] + ", ";
		}
		str += members[members.length - 1] + "]";
		return "score: " + score + " members: " + members.length + " " + str;
	}
}
