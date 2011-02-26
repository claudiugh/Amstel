package nl.vu.cs.amstel.graph.io;

public class TextFilePartition implements InputPartition {
	
	private static final long serialVersionUID = 2158801706663347676L;
	
	String filename;
	long offset;
	long length;
	
	public TextFilePartition(String filename, long offset, long length) {
		this.filename = filename;
		this.offset = offset;
		this.length = length;
	}
	
	public String toString() {
		return filename + ": from " + offset + " " + length + " bytes";
	}
}

