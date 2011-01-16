package nl.vu.cs.amstel.graph;

import java.lang.reflect.Constructor;

import org.apache.log4j.Logger;

public class VertexValueFactory<V> {

	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	
	protected Class<V> vertexValueClass;
	
	public VertexValueFactory(Class<V> vertexValueClass) {
		this.vertexValueClass = vertexValueClass;
	}
	
	public V create() {
		try {
			return vertexValueClass.newInstance();
		} catch (InstantiationException e) {
			logger.fatal("Error instantianting vertex value object", e);
		} catch (IllegalAccessException e) {
			logger.fatal("", e);
		}
		return null;
	}
	
	public V create(int value) {
		try {
			Constructor<V> constr =
				vertexValueClass.getConstructor(new Class[]{int.class});
			return constr.newInstance(value);
		} catch (Exception e) {
			logger.fatal("Error trying to create vertex value from int", e);
		}
		return null;
	}
}
