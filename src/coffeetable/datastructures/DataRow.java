package coffeetable.datastructures;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import coffeetable.interfaces.RowUtilities;
import coffeetable.interfaces.VectorUtilities;
import coffeetable.math.Infinite;
import coffeetable.math.MissingValue;
import coffeetable.math.TheoreticalValue;
import coffeetable.utils.SchemaMismatchException;


@SuppressWarnings("rawtypes")
public class DataRow extends ArrayList implements java.io.Serializable, VectorUtilities, RowUtilities {
	private static final long serialVersionUID = 3148244157795837127L;
	private String name;
	private boolean schemaFound = false;
	private Schema schema = new Schema();
	
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
		/* Originally returned schema.containsNAs() but if the 
		 * DataTable is rendered, the schema will no longer contain
		 * NAs. */
		if(this.isEmpty())
			return false;
		for(Object t : this) {
			if(TheoreticalValue.isTheoretical(t))
				return true;
		}
		return false;
	}
	
	public int countMissingValues() {
		/* Don't call containsNA() because then have to loop through
		 * twice. */
		if(this.isEmpty())
			return 0;
		int sum = 0;
		for(Object t : this) {
			if(TheoreticalValue.isTheoretical(t))
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
		clone.schemaFound = this.schemaFound;
		return clone;
	}
	
	public boolean equals(Object o) {
		if(!(o instanceof DataRow))
			return false;
		return o.hashCode() == this.hashCode();
	}
	
	public int hashCode() {
		int h = 0;
		Iterator i = iterator();
		while (i.hasNext()) {
			Object obj = i.next();
			if (obj != null)
				h += obj.hashCode();
		}
		return h^this.size();
	}
	
	/**
	 * If there is only one class, pass it to the datacolumn it is
	 * transforming to for caching purposes.
	 * @return the class of the DataRow contents if it is singular
	 */
	protected final Class<? extends Object> identifyClass() {
		return typeSafetyList().getContentClass();
	}
	
	public String name() {
		return name;
	}
	
	public void print() {
		System.out.println(this.toString());
	}
	
	public final static DataRow readFromSerializedObject(FileInputStream fileIn) throws IOException, ClassNotFoundException {
		DataRow d = null;
		ObjectInputStream in = new ObjectInputStream(fileIn);
		d = (DataRow) in.readObject();
		in.close();
		fileIn.close();
		return d;
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
		schemaFound = false;
	}
	
	/**
	 * returns the schema of the DataRow
	 */
	public Schema schema() {
		if(!(null == schema) && schemaFound)
			return schema;
		return typeSafetyList();
	}
	
	private boolean schemaCheck(Object element, int index) {
		if(null==schema)
			return true;
		if(TheoreticalValue.class.isAssignableFrom(schema.get(index)) ^ TheoreticalValue.isTheoretical(element)) {
			if(!TheoreticalValue.isTheoretical(element)) { //Schema is the one that has a Missing Value
				schema.set(index, element.getClass()); //Fix dat up
			}
			return true;
		}
		if(TheoreticalValue.class.isAssignableFrom(schema.get(index)) && TheoreticalValue.isTheoretical(element)) {
			return true;
		}
		return schema.get(index).equals(element.getClass());
	}
	
	
	@SuppressWarnings("unchecked")
	/**
	 * Set the value of the Object at the specified index
	 * to the element param
	 * @param element - the new value of the specified index
	 * @param index - the index at which to set a new value
	 * @return the old value of the specified index
	 */
	public Object set(int index, Object element) {
		if(!schemaCheck(element, index))
			throw new SchemaMismatchException("New object does not match schema");
		//Don't need to do the "RowUPdate()" because already customly checking schema changes here
		return super.set(index, element);
	}
	
	/**
	 * Assign a name for the row
	 */
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
		schemaFound = true;
	}
	
	/**
	 * Will convert the DataRow to DataColumn
	 * @return a DataColumn-transformed DataRow
	 */
	public DataColumn toDataColumn() {
		if(typeSafetyList().isSingular()) {
			//DataColumn d = new DataColumn( Arrays.asList(super.toArray()) , name );
			DataColumn d = new DataColumn(this);
			return d;
		} else throw new SchemaMismatchException("Cannot convert DataRow of multiple class types to DataColumn");
	}
	
	public String toString() {
		return name + " : " + super.toString();
	}
	
	/**
	 * Identifies the schema of the row
	 * @return
	 */
	private final Schema typeSafetyList() {
		if(schemaFound)
			return schema;
		Schema l = new Schema();
		for( int i = 0; i < super.size(); i++ ) {
			Class<?> cl = TheoreticalValue.isTheoretical(super.get(i)) ? 
					(MissingValue.isNA(super.get(i)) ? MissingValue.class : Infinite.class) 
					//TODO: ADD NaN WHEN CLASS DONE
					: super.get(i).getClass();
			l.add(cl);
		}
		schema = l;
		schemaFound = true;
		return l;
	}
	
	@SuppressWarnings("unchecked")
	public Set<?> unique() {
		return new HashSet(this);
	}
	
	/**
	 * Writes a serialized DataRow object from this instance,
	 * returns true if successful
	 * @throws IOException
	 * @param path - the path to which to write
	 * @return true if the operation was successful
	 */
	public final boolean writeObject(String path) throws IOException {
		if(null == path || path.isEmpty()) {
			path = "/tmp/datarow.ser"; 
			System.out.println("Path was empty, saving to "+path);
		} else if(!path.endsWith(".ser"))
			path += ".ser";
			
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(this);
		out.close();
		fileOut.close();
		return new File(path).exists();
	}
}
