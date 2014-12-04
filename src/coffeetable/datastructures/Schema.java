package coffeetable.datastructures;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;

import coffeetable.math.MissingValue;


/**
 * A object that defines the "schema" of the DataRow, supports
 * Schema-safety match checking, etc.
 * @author Taylor G Smith
 */
@SuppressWarnings("rawtypes")
public class Schema extends LinkedList<Class<?>> implements Serializable {
	private static final long serialVersionUID = -2991576118127902417L;
	
	public Schema() {
		super();
	}
	
	public Schema(LinkedList<Class<?>> schema) {
		this();
		super.addAll(schema);
	}
	
	public boolean isNumeric() {
		if(null==this)
			return false;
		for(Class c : this) {
			if(!Number.class.isAssignableFrom(c))
				return false;
		}
		return true;
	}
	
	public boolean isSafe(Schema sch) {
		if(!(this.contains(MissingValue.class) || sch.contains(MissingValue.class)))
			return this.equals(sch);
		if(sch.size() != this.size())
			return false;
		HashMap<Integer, Class> postcheckSetter = new HashMap<Integer, Class>();
		//Check...
		for(int i = 0; i < sch.size(); i++) {
			Class<?> c = sch.get(i);
			if(c.equals(MissingValue.class))
				continue;
			else if(this.get(i).equals(MissingValue.class)) {
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
	
	public String toString() {
		return super.toString();
	}
}
