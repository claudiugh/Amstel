package nl.vu.cs.amstel.msg;

import org.apache.log4j.Logger;

import nl.vu.cs.amstel.user.Combiner;
import nl.vu.cs.amstel.user.MessageValue;

public class MessageFactory<M extends MessageValue> {
	
	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	private Class<M> messageClass;
	private Class<? extends Combiner<M>> combinerClass = null;
	
	public MessageFactory(Class<M> messageClass) {
		this.messageClass = messageClass;
	}
	
	public Class<M> getMessageClass() {
		return messageClass;
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
	
	public void setCombinerClass(Class<? extends Combiner<M>> combinerClass) {
		this.combinerClass = combinerClass;
	}
	
	public boolean hasCombiner() {
		return combinerClass != null;
	}
	
	public Combiner<M> createCombiner() {
		if (combinerClass == null) {
			return null;
		}
		try {
			return combinerClass.newInstance();
		} catch (InstantiationException e) {
			logger.fatal(e);
		} catch (IllegalAccessException e) {
			logger.fatal(e);
		}
		return null;
	}
}
