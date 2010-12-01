package nl.vu.cs.amstel.msg;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.user.MessageValue;

public class MessageFactory<M extends MessageValue> {
	
	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private Class<M> messageClass;
	
	public MessageFactory(Class<M> messageClass) {
		this.messageClass = messageClass;
	}
	
	public M create() {
		try {
			M msg = messageClass.newInstance();
			return msg;
		} catch (InstantiationException e) {
			logger.fatal(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		// we get here only in case of an exception
		return null;
	}
}
