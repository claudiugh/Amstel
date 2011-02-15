package nl.vu.cs.amstel.user;

import org.apache.log4j.Logger;

public abstract class Aggregator<T extends Value> {

	protected static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	protected boolean sticky = false;
	protected String name;
	protected T value = null;
	protected Class<T> valueClass;
	
	public Aggregator(Class<T> valueClass, String name) {
		this.name = name;
		this.valueClass = valueClass;
	}
	
	public void reset() {
		this.value = null;
	}
	
	public String getName() {
		return name;
	}

	public Value getValue() {
		return value;
	}
	
	public boolean hasValue() {
		return value != null;
	}
	
	public void setSticky(boolean sticky) {
		this.sticky = sticky;
	}
	
	public boolean isSticky() {
		return sticky;
	}
	
	public T newValue() {
		try {
			T valueInstance = valueClass.newInstance();
			return valueInstance;
		} catch (Exception e) {
			logger.fatal("Error instantianting new value for Aggregator " 
					+ name, e);
		}
		return null;
	}
	
	public abstract void init(T value);
	
	public abstract void combine(T value);
}