package nl.vu.cs.amstel.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.graph.OutEdgeIterator;
import nl.vu.cs.amstel.user.IntValue;
import nl.vu.cs.amstel.user.Vertex;

public class SemiClusteringVertex 
	extends Vertex<SemiClusterList, IntValue, SemiClusterMessage> {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	public static final int CMAX = 8;
	public static final int SEND_MAX = 32;
	public static final int CLUSTER_MAX_SIZE = 16;
	
	private static ArrayList<SemiCluster> newClusters =
		new ArrayList<SemiCluster>();
	private static SemiClusterList tmpClist = new SemiClusterList();
	private static SemiCluster[] tmpMerge = new SemiCluster[CMAX];
	
	private int sumEdgesCost() {
		int sum = 0;
		OutEdgeIterator<IntValue> iter = getOutEdgeIterator();
		while (iter.hasNext()) {
			sum += iter.getEdgeValue().value;
			iter.next();
		}
		return sum;
	}
	
	private void init() {
		SemiCluster self = new SemiCluster(getID(), sumEdgesCost());
		tmpClist.clusters = new SemiCluster[] {self};
		SemiClusterMessage msg = newMessage();
		msg.clustersList = tmpClist;
		sendToAll(msg);	
	}
	
	private SemiCluster addSelfTo(SemiCluster cluster) {
		String[] members = Arrays.copyOf(cluster.members, cluster.size() + 1);
		members[members.length - 1] = getID();
		int internalWeight = cluster.internalWeight;
		int boundaryWeight = cluster.boundaryWeight;
		OutEdgeIterator<IntValue> iter = getOutEdgeIterator();
		while (iter.hasNext()) {
			String target = iter.getEdgeTarget();
			int cost = iter.getEdgeValue().value;
			if (cluster.contains(target)) {
				// we count the backward link (the graph is bidirectional)
				internalWeight += 2 * cost;
				// the backward link doesn't count as boundary link anymore
				boundaryWeight -= cost;
			} else {
				boundaryWeight += cost;
			}
			iter.next();
		}
		return new SemiCluster(members, internalWeight, boundaryWeight);
	}
	
	private static void removeDuplicates(ArrayList<SemiCluster> clusters) {
		int i = 0;
		while (i < clusters.size() - 1) {
			if (clusters.get(i).equals(clusters.get(i + 1))) {
				clusters.remove(i);
			} else {
				i++;
			}
		}
	}
	
	private boolean updateLocalClusters(ArrayList<SemiCluster> clusters) {
		SemiClusterList localList = getValue();
		if (clusters.size() == 0) {
			return false;
		}
		SemiCluster prevLast = localList.getLast();
		// do merging having in mind that clusters may contain grouped
		// duplicates
		int i = 0, j = 0, k = 0;
		while (i < clusters.size() && j < localList.clusters.length
				&& k < CMAX) {
			// skip the duplicates
			while ((i < clusters.size() - 1) && 
					clusters.get(i).compareTo(clusters.get(i + 1)) == 0) {
				i++;
			}
			if (clusters.get(i).contains(getID())) {
				int cmp = clusters.get(i).compareTo(localList.clusters[j]);
				if (cmp < 0) {
					tmpMerge[k] = clusters.get(i);
					i++;
					k++;
				} else if (cmp > 0) {
					tmpMerge[k] = localList.clusters[j];
					j++;
					k++;
				} else {
					tmpMerge[k] = localList.clusters[j];
					i++;
					j++;
					k++;
				}
			} else {
				i++;
			}
		}
		// copy rest
		if (i == clusters.size()) {
			while (j < localList.clusters.length && k < CMAX) {
				tmpMerge[k] = localList.clusters[j];
				j++;
				k++;
			}
		} else if (j == localList.clusters.length) {
			while (i < clusters.size() && k < CMAX) {
				tmpMerge[k] = clusters.get(i);
				i++;
				k++;
				while ((i < clusters.size()) && 
						clusters.get(i - 1).compareTo(clusters.get(i)) == 0) {
					i++;
				}
			}
		}
		// update from tmp to local list
		if (localList.clusters.length < k) {
			localList.clusters = new SemiCluster[k];
		}
		for (i = 0; i < k; i++) {
			localList.clusters[i] = tmpMerge[i];
		}
		// figure out if we have updated the local list
		if (prevLast == null || !localList.getLast().equals(prevLast)) {
			return true;
		}
		return false;
	}
	
	@Override
	public void compute(Iterable<SemiClusterMessage> messages) {
		if (getSuperstep() == 0) {
			init();
			return;
		}
		newClusters.clear();
		// iterate over the received semi-clusters
		for (SemiClusterMessage msg : messages) {
			for (SemiCluster cluster : msg.clustersList.clusters) {
				newClusters.add(cluster);
			}
		}
		Collections.sort(newClusters);
		removeDuplicates(newClusters);
		
		int newCount = newClusters.size();
		for (int i = 0; i < newCount; i++) {
			SemiCluster cluster = newClusters.get(i);
			if (cluster.size() < CLUSTER_MAX_SIZE 
					&& !cluster.contains(getID())) {
				newClusters.add(addSelfTo(cluster));
			}
		}
		Collections.sort(newClusters);
		SemiClusterMessage localMsg = newMessage();
		
		// send to neighbors the best 
		int toSend = 
			newClusters.size() < SEND_MAX ? newClusters.size() : SEND_MAX;
		tmpClist.clusters = new SemiCluster[toSend];
		for (int i = 0; i < toSend; i++) {
			tmpClist.clusters[i] = newClusters.get(i);
		}
		localMsg.clustersList = tmpClist;
		
		boolean updated = updateLocalClusters(newClusters);
		if (!updated) {
			outputAggregate("BestSemiClusters", getValue());
			voteToHalt();
		} else {
			sendToAll(localMsg);
		}
		
		// flush the static variables
		newClusters.clear();
	}

}
