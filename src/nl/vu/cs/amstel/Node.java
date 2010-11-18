package nl.vu.cs.amstel;

import ibis.ipl.Ibis;
import ibis.ipl.IbisCapabilities;
import ibis.ipl.IbisFactory;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;

public class Node {
	
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
	
	private void run() throws Exception {
		 Ibis ibis = IbisFactory.createIbis(ibisCapabilities, null, 
				 W2M_PORT, M2W_PORT, W2W_PORT);		 
	     // Elect a server
	     IbisIdentifier master = ibis.registry().elect("Master");
	     
	     if (master.equals(ibis.identifier())) {
	    	 new Master(ibis, 3);
	     } else {
	    	 new Worker(ibis, master);
	     }
	}
	
	public static void main(String args[]) {
		try {
			new Node().run();
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
}
