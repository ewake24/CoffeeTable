package coffeetable.datastructures;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import coffeetable.math.Infinite;
import coffeetable.math.MissingValue;
import coffeetable.math.TheoreticalValue;


/**
 * A object that defines the "schema" of the DataRow, supports
 * Schema-safety match checking, etc.
 * @author Taylor G Smith
 */
@SuppressWarnings("rawtypes")
public final class Schema extends LinkedList<Class<?>> implements Serializable {
	private static final long serialVersionUID = -2991576118127902417L;
	
	public Schema() {
		super();
	}
	
	public Schema(LinkedList<Class<?>> schema) {
		this();
		super.addAll(schema);
	}
	
	/**
	 * Identifies the singular class of the schema (if singular)
	 * @return the singular class of the schema, null if not singular
	 */
	protected Class<?> getContentClass() {
		if(!this.isSingular())
			return null;
		return this.get(0);
	}
	
	/**
	 * Returns whether the schema contains any NA values
	 * @return true if there are any TheoreticalValues in the schema
	 */
	public boolean containsNAs() {
		if(this.isEmpty())
			return false;
		for(Class c : this)
			if(TheoreticalValue.class.isAssignableFrom(c))
				return true;
		return false;
	}
	
	/**
	 * Returns whether the schema is completely numeric
	 * @return true if all classes are numeric
	 */
	public boolean isNumeric() {
		if(null==this)
			return false;
		for(Class c : this) {
			if(!Number.class.isAssignableFrom(c))
				return false;
		}
		return true;
	}
	
	/**
	 * Determines whether the schema matches another schema
	 * @param sch
	 * @return true if the schemas match
	 */
	public boolean isSafe(Schema sch) {
		if(!(this.containsNAs() || sch.containsNAs()))
			return this.equals(sch);
		if(sch.size() != this.size())
			return false;
		HashMap<Integer, Class> postcheckSetter = new HashMap<Integer, Class>();
		//Check...
		for(int i = 0; i < sch.size(); i++) {
			Class<?> c = sch.get(i);
			if(TheoreticalValue.class.isAssignableFrom(c))
				continue;
			else if(TheoreticalValue.class.isAssignableFrom(this.get(i))) {
				postcheckSetter.put(i, c);
				continue;
			} else if(!c.equals(this.get(i))) {
				return false;
			}
		}
		
		//Amend schema if needed...
		if(!postcheckSetter.isEmpty()) {
			for(Integer key : postcheckSetter.keySet())
				this.set(key, postcheckSetter.get(key));
		}
		return true;
	}
	
	/**
	 * Returns whether the schema only contains one class
	 * @return true if all classes are equal
	 */
	public boolean isSingular() {
		HashSet<Class> sdf = new HashSet<Class>(this);
		if(sdf.size() ==1 )
			return true;
		if(sdf.contains(MissingValue.class))
			sdf.remove(MissingValue.class);
		if(sdf.contains(Infinite.class))
			sdf.remove(Infinite.class);
		return (sdf.size()==1);
	}
	
	public String toString() {
		return super.toString();
	}
}
