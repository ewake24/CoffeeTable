package coffeetable.datastructures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;

import coffeetable.interfaces.VectorUtilities;

public abstract class Vector<T extends Comparable<? super T> & java.io.Serializable> extends ArrayList<T> 
	implements VectorUtilities<T> {
	private static final long serialVersionUID = 5665797156373871723L;
	protected String name;
	
	public Vector(int initSize) {
		super(initSize);
	}
	
	/**
	 * Generates a hashcode based on the container contents, 
	 * scaled by the size of the container
	 */
	public int hashCode() {
		int h = 0;
		Iterator<T> i = iterator();
		while (i.hasNext()) {
			Object obj = i.next();
			if (obj != null)
				h += obj.hashCode();
		}
		return h^this.size();
	}
	
	public String name() {
		return name;
	}
	
	public String toString() {
		return name + " : " + super.toString();
	}
	
	public LinkedHashSet<T> unique() {
		return new LinkedHashSet<T>(this);
	}
	
	public abstract boolean containsNA();
	public abstract int countMissingValues();
	public abstract void print();
	public abstract void setName(String s);
}
