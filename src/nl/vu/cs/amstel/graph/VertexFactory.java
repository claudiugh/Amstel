package nl.vu.cs.amstel.graph;

import java.lang.reflect.Constructor;

import nl.vu.cs.amstel.user.NullValue;

import org.apache.log4j.Logger;

public class VertexFactory<V, E> {

	private static Logger logger = Logger.getLogger("nl.vu.cs.amstel");
	private static boolean nullEdgeValues;
	
	public Class<V> vertexValueClass;
	public Class<E> edgeValueClass;
	
	public VertexFactory(Class<V> vertexValueClass, Class<E> edgeValueClass) {
		this.vertexValueClass = vertexValueClass;
		this.edgeValueClass = edgeValueClass;
		nullEdgeValues = edgeValueClass.equals(NullValue.class);
	}
	
	public V createValue() {
		try {
			return vertexValueClass.newInstance();
		} catch (InstantiationException e) {
			logger.fatal("Error instantianting vertex value object", e);
		} catch (IllegalAccessException e) {
			logger.fatal("", e);
		}
		return null;
	}
	
	public V createValue(int value) {
		try {
			Constructor<V> constr =
				vertexValueClass.getConstructor(new Class[]{int.class});
			return constr.newInstance(value);
		} catch (Exception e) {
			logger.fatal("Error trying to create vertex value from int", e);
		}
		return null;
	}
	
	public V createValue(String value) {
		try {
			Constructor<V> constr =
				vertexValueClass.getConstructor(new Class[]{String.class});
			return constr.newInstance(value);
		} catch (Exception e) {
			logger.fatal("Error trying to create vertex value from String", e);
		}
		return null;
	}
	
	public E createEdgeValue() {
		try {
			return edgeValueClass.newInstance();
		} catch (Exception e) {
			logger.fatal("Error instantiating edge value", e);
		}
		return null;
	}
	
	public E createEdgeValue(int value) {
		Constructor<E> constr;
		try {
			constr = edgeValueClass.getConstructor(new Class[]{int.class});
			return constr.newInstance(value);
		} catch (Exception e) {
			logger.error("Error instantiating edge value from int", e);
		}
		return null;
	}
	
	public E createEdgeValue(String value) {
		Constructor <E> constr;
		try {
			constr = edgeValueClass.getConstructor(new Class[]{String.class});
			return constr.newInstance(value);
		} catch (Exception e) {
			logger.error("Error instantiating edge value from String", e);
		}
		return null;
	}
	
	public static boolean hasNullEdgeValue() {
		return nullEdgeValues;
	}
}
