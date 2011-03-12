package nl.vu.cs.amstel.examples;

import java.util.Arrays;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.user.Aggregator;

public class TopClusterAggregator extends Aggregator<SemiClusterList> {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");

	private SemiCluster[] tmp = new SemiCluster[SemiClusteringVertex.CMAX];
	
	public TopClusterAggregator(String name) {
		super(SemiClusterList.class, name);
	}
	
	/**
	 * We need to do a merge between the current value and the given value 
	 */
	@Override
	public void combine(SemiClusterList value) {
		int i = 0, j = 0, k = 0;
		int ilen = this.value.clusters.length;
		int jlen = value.clusters.length;
		while (i < ilen && j < jlen && k < tmp.length) {
			int cmp = this.value.clusters[i].compareTo(value.clusters[j]);
			if (cmp < 0) {
				tmp[k] = this.value.clusters[i];
				i++;
				k++;
			} else if (cmp > 0) {
				tmp[k] = value.clusters[j];
				j++;
				k++;
			} else {
				tmp[k] = this.value.clusters[i];
				i++;
				j++;
				k++;
			}
		}
		if (i == ilen) {
			while (j < jlen && k < tmp.length) {
				tmp[k] = value.clusters[j];
				j++;
				k++;
			}
		} else if (j == jlen) {
			while (i < ilen && k < tmp.length) {
				tmp[k] = this.value.clusters[i];
				i++;
				k++;
			}
		}
		if (this.value.clusters.length != k) {
			this.value.clusters = new SemiCluster[k];
		}
		for (i = 0; i < k; i++) {
			this.value.clusters[i] = tmp[i];
		}
	}

	@Override
	public void init(SemiClusterList value) {
		this.value = new SemiClusterList();
		this.value.clusters = 
			Arrays.copyOf(value.clusters, value.clusters.length);
	}

}
