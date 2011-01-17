package nl.vu.cs.amstel;

import nl.vu.cs.amstel.user.Combiner;
import nl.vu.cs.amstel.user.MessageValue;
import nl.vu.cs.amstel.user.Value;
import nl.vu.cs.amstel.user.Vertex;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;

public class Node<V extends Value, E extends Value, M extends MessageValue> {
	
    public static PortType W2M_PORT = 
    	new PortType(PortType.COMMUNICATION_RELIABLE, 
    			PortType.SERIALIZATION_DATA, PortType.SERIALIZATION_OBJECT,
    			PortType.SERIALIZATION_OBJECT_IBIS, PortType.RECEIVE_EXPLICIT,
    			PortType.CONNECTION_MANY_TO_ONE);
    public static PortType M2W_PORT = 
    	new PortType(PortType.COMMUNICATION_RELIABLE,
    			PortType.SERIALIZATION_OBJECT, PortType.RECEIVE_EXPLICIT,
    			PortType.CONNECTION_ONE_TO_MANY);
    public static PortType W2W_PORT =
    	new PortType(PortType.COMMUNICATION_RELIABLE, 
    			PortType.SERIALIZATION_DATA, PortType.RECEIVE_EXPLICIT,
    			PortType.CONNECTION_MANY_TO_ONE);
    
    IbisCapabilities ibisCapabilities = new IbisCapabilities(
            IbisCapabilities.ELECTIONS_STRICT);	
	
    private Ibis ibis;
    private AmstelNode<V, M> node;
    
    public Node(int nodes, Class<? extends Vertex<V, E, M>> vertexClass,
    		Class<V> vertexValueClass,
    		Class<E> edgeValueClass,
			Class<M> messageClass) throws Exception {
    	ibis = IbisFactory.createIbis(ibisCapabilities, null, 
				W2M_PORT, M2W_PORT, W2W_PORT);		 
	    // Elect a server
	    IbisIdentifier master = ibis.registry().elect("Master");
    	if (master.equals(ibis.identifier())) {
    		// the number of workers is the total number of nodes excluding the
    		// the master node
    		node = new Master<V, M>(ibis, nodes - 1);
	    } else {
	    	node = new Worker<V, E, M>(ibis, master, vertexClass, 
	    			vertexValueClass, edgeValueClass, messageClass);
	    }
    }
    
    public void setCombiner(Class<? extends Combiner<M>> combinerClass) {
    	node.setCombiner(combinerClass);
    }
    
	public void run() {
		try {
			node.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
