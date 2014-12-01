package coffeetable.datastructures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import coffeetable.interfaces.RowUtilities;
import coffeetable.interfaces.VectorUtilities;
import coffeetable.utils.SchemaMismatchException;


@SuppressWarnings("rawtypes")
public class DataRow extends ArrayList implements VectorUtilities, RowUtilities {
	private static final long serialVersionUID = 3148244157795837127L;
	private String name;
	private transient Schema schema = new Schema();
	
	public DataRow(int initSize) {
		super(initSize);
		name = "DataRow";
	}
	
	public DataRow() {
		/* Init methods if any */
		this(15);
	}
	
	public DataRow(Collection arg0) {
		this(arg0.size());
		this.addAll(arg0);
	}

	public DataRow(Collection arg0, String name) {
		this(arg0.size());
		this.addAll(arg0);
		setName(name);
	}
	
	public DataRow(String name) {
		this();
		setName(name);
	}
	
	@SuppressWarnings("unchecked")
	public boolean addFromTrusted(Object element) {
		return super.add(element);
	}
	
	@SuppressWarnings("unchecked")
	public boolean add(Object element) {
		rowUpdate();
		return super.add(element);
	}
	
	@SuppressWarnings("unchecked")
	public void add(int index, Object element) {
		rowUpdate();
		super.add(index, element);
	}
	
	@SuppressWarnings("unchecked")
	public boolean addAll(Collection arg0) {
		rowUpdate();
		return super.addAll(arg0);
	}
	
	@SuppressWarnings("unchecked")
	public boolean addAll(int index, Collection arg1) {
		rowUpdate();
		return super.addAll(index, arg1);
	}
	
	public void clear() {
		schema = null;
	}
	
	public boolean containsNA() {
		if(this.isEmpty())
			return false;
		for(Object t : this) {
			if(MissingValue.isNA(t))
				return true;
		}
		return false;
	}
	
	public int countMissingValues() {
		if(this.isEmpty())
			return 0;
		int sum = 0;
		for(Object t : this) {
			if(MissingValue.isNA(t))
				sum += 1;
		}
		return sum;
	}
	
	public Object clone() {
		DataRow clone = null;
		try {
			clone = (DataRow) super.clone();
		} catch(Exception e) {
			throw new InternalError();
		}
		
		if(!(null==schema)) {
			clone.schema = schema;
		} else {
			clone.schema = null;
		}
		return clone;
	}
	
	/**
	 * If there is only one class, pass it to the datacolumn it is
	 * transforming to for caching purposes.
	 * @return the class of the DataRow contents if it is singular
	 */
	protected final Class<? extends Object> identifyClass() {
		return typeSafetyList().get(0);
	}
	
	@SuppressWarnings("unchecked")
	private final boolean isSameClass() {
		HashSet<Class> sdf = new HashSet(typeSafetyList());
		return (sdf.size()==1) || (sdf.size()==2 && sdf.contains(MissingValue.class));
	}
	
	public String name() {
		return name;
	}
	
	public void print() {
		System.out.println(this.toString());
	}
	
	/**
	 * Removes the object from the DataColumn
	 */
	public boolean remove(Object arg0) {
		rowUpdate();
		return super.remove(arg0);
	}
	
	public Object remove(int index) {
		rowUpdate();
		return super.remove(index);
	}
	
	@SuppressWarnings("unchecked")
	public boolean removeAll(Collection arg0) {
		rowUpdate();
		return super.removeAll(arg0);
	}
	
	private final void rowUpdate() {
		schema = null;
	}
	
	public Schema schema() {
		if(!(null == schema))
			return schema;
		return typeSafetyList();
	}
	
	private boolean schemaCheck(Object element, int index) {
		if(null==schema)
			return true;
		if(schema.get(index).equals(MissingValue.class) ^ MissingValue.isNA(element)) {
			if(!MissingValue.isNA(element)) { //Schema is the one that has a Missing Value
				schema.set(index, element.getClass()); //Fix dat up
			}
			return true;
		}
		if(schema.get(index).equals(MissingValue.class) && MissingValue.isNA(element)) {
			return true;
		}
		return schema.get(index).equals(element.getClass());
	}
	
	@SuppressWarnings("unchecked")
	public Object set(int index, Object element) {
		if(!schemaCheck(element, index))
			throw new SchemaMismatchException("New object does not match schema");
		//Don't need to do the "RowUPdate()" because already customly checking schema changes here
		return super.set(index, element);
	}
	
	public void setName(String name) {
		this.name = (null == name || name.isEmpty()) ? "DataRow" : 
						(name.equals("DataColumn") ? "DataRow" : name); 
	}
	
	/**
	 * Used as a helper method in DataTable in case MissingValue is
	 * within the schema
	 * @param sch
	 */
	protected void setSchema(Schema sch) {
		if(sch.isEmpty())
			return;
		schema = sch;
	}
	
	@SuppressWarnings("unchecked")
	public DataColumn toDataColumn() {
		if(isSameClass()) {
			DataColumn d = new DataColumn( Arrays.asList(super.toArray()) , name );
			return d;
		} else throw new SchemaMismatchException("Cannot convert DataRow of multiple class types to DataColumn");
	}
	
	public String toString() {
		return name + " : " + super.toString();
	}
	
	private final Schema typeSafetyList() {
		Schema l = new Schema();
		for( int i = 0; i < super.size(); i++ ) {
			Class<?> cl = MissingValue.isNA(super.get(i)) ? MissingValue.class : super.get(i).getClass();
			l.add(cl);
		}
		schema = l;
		return l;
	}
}
