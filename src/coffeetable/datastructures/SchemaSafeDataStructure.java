package coffeetable.datastructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import coffeetable.utils.DimensionMismatchException;
import coffeetable.utils.SchemaMismatchException;

/**
 * A super, abstract class for DataTable providing all schema operations
 * @author Taylor G Smith
 */
@SuppressWarnings("rawtypes")
public abstract class SchemaSafeDataStructure implements Serializable {
	private static final long serialVersionUID = -5011930758106873757L;
	protected final static int defaultNumRows = 25;
	protected final static int defaultNumCols = 10;
	
	protected ArrayList<DataColumn> cols;
	protected ArrayList<DataRow> rows;
	protected Schema schema = null;
	protected ArrayList<Exception> exceptionLog;
	private HashMap<String, Integer> options;
	private boolean isRendered; //cache rendering operations
	
	public SchemaSafeDataStructure() { this(defaultNumRows,defaultNumCols); }
	public SchemaSafeDataStructure(int numRows, int numCols) {
		if(numRows < 0 || numCols < 0) 
			throw new IllegalArgumentException("Cannot instantiate DataStructure of negative dimensions");
		initLists(numRows,numCols);
		exceptionLog = new ArrayList<Exception>();
		options = setInitOptions();
	}
	
	/** A set of initialization methods that allow flexibility in the
	 * size of initialized lists for cols/rows.
	 */
	private void initLists(int r, int c) {
		cols = new ArrayList<DataColumn>(r);
		rows = new ArrayList<DataRow>(c);
	}	
	
	/**
	 * Allows extended classes to add options to the options container.
	 * If the option already exists, will throw ConcurrentModificationException,
	 * as the established keys are critical to the performance of the struct
	 * @param name
	 * @param value
	 */
	protected final void addOption(String name, int value) {
		if(options.containsKey(name))
			throw new ConcurrentModificationException("Cannot add existing key ("+name+") to options frame. Use setOptions(name,value) instead");
		options.put(name, value);
	}
	
	private final static HashMap<String,Integer> setInitOptions() {
		HashMap<String, Integer> options = new HashMap<String,Integer>();
		options.put("default.num.rows",defaultNumRows);	//Num rows to instantiate cols with
		options.put("default.num.cols",defaultNumCols);	//Num cols to instantiate rows with
		return options;
	}
	
	/**
	 * Inner class for the cleaner management of content -- adds, schema assignments, etc.
	 * @author Taylor G Smith
	 */
	@SuppressWarnings("unchecked")
	final static class ContentFactory {
		static void addEmptyColsWithInitPopulate(SchemaSafeDataStructure table, DataRow row, boolean trusted) {
			for( int i = 0; i < row.size(); i++ ) {
				table.cols.add( trusted ? 
						new DataColumn(true,table.options.get("default.num.rows")) : 
							new DataColumn( table.options.get("default.num.rows")) );
				if(trusted)
					table.cols.get(i).addFromTrusted((Comparable)row.get(i));
				else table.cols.get(i).add((Comparable)row.get(i));
			}
		}
		
		static void addEmptyRowsWithInitPopulate(SchemaSafeDataStructure table, DataColumn col) {
			for( int i = 0; i < col.size(); i++ ) {
				table.rows.add(new DataRow(col.size()));
				table.rows.get(i).add(col.get(i));
			}
		}
		
		static void addToExistingCols(SchemaSafeDataStructure table, DataRow row, boolean trusted) {
			dimensionCheckRow(row, table);
			for( int i = 0; i < row.size(); i++ ) {
				if(trusted)
					table.cols.get(i).addFromTrusted((Comparable) row.get(i));
				else table.cols.get(i).add((Comparable) row.get(i));
			}
		}
		
		static void addToExistingColsAtIndex(SchemaSafeDataStructure table, DataRow row, boolean trusted, int index) {
			dimensionCheckRow(row, table);
			for( int i = 0; i < row.size(); i++ ) {
				if(trusted)
					table.cols.get(i).addFromTrusted((Comparable) row.get(i), index);
				else table.cols.get(i).add(index, (Comparable) row.get(i));
			}
		}
		
		static void addToExistingRows(SchemaSafeDataStructure table, DataColumn col ) {
			dimensionCheckCol(col, table);
			for( int i = 0; i < col.size(); i++ )
				table.rows.get(i).add(col.get(i));
		}
		
		static void addToExistingRowsAtIndex(SchemaSafeDataStructure table, DataColumn col, int index) {
			dimensionCheckCol(col, table);
			for( int i = 0; i < col.size(); i++ )
				table.rows.get(i).add(index,col.get(i));
		}
		
		static void dimensionCheckCol(DataColumn col, SchemaSafeDataStructure table) {
			if(!(col.size() == table.nrow()))
				throw new DimensionMismatchException("Column length does not match number of rows");
		}
		
		static void dimensionCheckRow(DataRow row, SchemaSafeDataStructure table) {
			if(!(row.size() == table.ncol()))
				throw new DimensionMismatchException("Row length does not match number of columns");
		}
		
		static void schemaAssignment(SchemaSafeDataStructure table, DataRow row) {
			if(null == table.schema || table.rows.isEmpty())
				table.schema = row.schema();
		}
	}
	
	/*--------------------------------------------------------------------*/
	/**
	 * Add a collection of DataColumns to the container
	 * @param columns
	 */
	public final void addAllColumns(Collection<DataColumn> columns) {
		for(DataColumn col : columns)
			this.addColumn(col);
	}
	
	/**
	 * Add a collection of DataRows to the container
	 * @param rows
	 */
	public final void addAllRows(Collection<DataRow> rows) {
		for(DataRow row : rows)
			this.addRow(row);
	}
	
	/**
	 * Add a single DataRow to the container
	 * @param row
	 */
	public final void addRow(DataRow row) {
		tableUpdate();
		ContentFactory.schemaAssignment(this,row);
		if(!schemaIsSafe(row.schema()))
			throw new SchemaMismatchException("Row schemas do not match");
		rows.add(row);
		if(cols.isEmpty())
			ContentFactory.addEmptyColsWithInitPopulate(this,row,false);
		else ContentFactory.addToExistingCols(this,row,false);
	}
	
	/**
	 * Add a single DataRow to the container at the specified index
	 * @param index
	 * @param row
	 */
	public final void addRow(int index, DataRow row) {
		tableUpdate();
		if(rows.isEmpty() || null == schema || index==rows.size()) {
			addRow(row); return;
		} else if(index > rows.size())
			throw new ConcurrentModificationException("Proposed index suggests non-concurrent row addition (would require rows of NA values)");
		else if(!schemaIsSafe(row.schema()))
			throw new SchemaMismatchException("Row schemas do not match");
		rows.add(index, row);
		ContentFactory.addToExistingColsAtIndex(this, row, false, index);
	}
	
	/**
	 * Add a single DataColumn to the container
	 * @param col
	 */
	public final void addColumn(DataColumn<?> col) {
		tableUpdate();
		if(cols.isEmpty() || null==schema)
			updateSchemaFromNew(col.contentClass());
		else 
			updateSchema(col.contentClass());
		cols.add(col);
		if(rows.isEmpty()) 
			ContentFactory.addEmptyRowsWithInitPopulate(this, col);
		else ContentFactory.addToExistingRows(this, col);
	}
	
	/**
	 * Add a single DataColumn to the container at the specified index
	 * @param index
	 * @param col
	 */
	public final void addColumn(int index, DataColumn<?> col) {
		tableUpdate();
		if(cols.isEmpty() || null==schema || index==cols.size()) {
			addColumn(col); return;
		} else if(index > cols.size()) {
			throw new ConcurrentModificationException("Proposed index suggests non-concurrent column addition (would require cols of NA values)");
		}
		updateSchemaAt(index, col.contentClass());
		cols.add(index,col);
		ContentFactory.addToExistingRowsAtIndex(this, col, index);
	}

	/**
	 * Clears all data from the container
	 */
	public void clear() {
		cols = new ArrayList<DataColumn>();
		rows = new ArrayList<DataRow>();
		schema = null;
		tableUpdate();
	}
	
	public final void clearColumnCaches() {
		//Reset column caches
		for(DataColumn c : cols)
			c.columnUpdate();
	}
	
	/**
	 * Returns a collection of the container's column names
	 * @return collection of the container's column names
	 */
	public Collection<String> columnNames() {
		Collection<String> names = new LinkedList<String>();
		for( DataColumn r : cols ) {
			names.add(r.name);
		}
		return names;
	}
	
	/**
	 * Returns a collection of the container's columns
	 * @return collection of the container's columns
	 */
	public final ArrayList<DataColumn> columns() {
		return cols;
	}
	
	/**
	 * Returns true if the container contains any NA values
	 */
	public final boolean containsNA() {
		if(cols.isEmpty())
			return false;
		for(DataColumn c : cols) {
			if(c.containsNA())
				return true;
		}
		return false;
	}
	
	/**
	 * Returns the number of MissingValues in the table
	 */
	public final int countMissingValues() {
		if(this.isEmpty())
			return 0;
		int sum = 0;
		for(DataColumn t : cols) {
			sum += t.countMissingValues();
		}
		return sum;
	}
	
	/**
	 * Returns the list of any exceptions that have been handled since the instantiation
	 * of the container instance (generally parsing exceptions if auto-detect type is
	 * enabled in the CsvParser)
	 * @return an ArrayList of exceptions the DataTable has encountered
	 */
	public final ArrayList<Exception> exceptionLog() {
		return exceptionLog;
	}
	
	/**
	 * @param index
	 * @return DataColumn at specified index
	 */
	public final DataColumn<?> getColumn(int index) {
		return cols.get(index);
	}
	
	/**
	 * Will throw an ArrayIndexOutOfBoundsException if the name is
	 * not found amongst the column names
	 * @param name
	 * @return DataColumn with specified name
	 */
	public final DataColumn<?> getColumn(String name) {
		ArrayList<String> names = new ArrayList<String>(columnNames());
		return this.getColumn(names.indexOf(name));
	}
	
	/**
	 * @param option
	 * @return the option parameter with the given name (ex: "default.num.rows")
	 */
	public final Integer getOption(String option) {
		if(!options.containsKey(option))
			return null;
		else return options.get(option);
	}
	
	/**
	 * The DataTable is governed by a set of 'options,' i.e., "default.num.rows,"
	 * etc. These affect the behavior of some features of the container.
	 * This method returns all keys in the options HashMap; the setOption()
	 * method will allow setting of option parameters
	 * @return the container's valid, settable options
	 */
	public final Set<String> getOptionKeys() {
		return options.keySet();
	}
	
	/**
	 * @param index
	 * @return DataRow at specified index
	 */
	public final DataRow getRow(int index) {
		return rows.get(index);
	}
	
	/**
	 * @param name
	 * @return DataRow with specified name
	 */
	public final DataRow getRow(String name) {
		ArrayList<String> names = new ArrayList<String>(rowNames());
		return this.getRow(names.indexOf(name));
	}

	/**
	 * If the container has encountered any exceptions, the log stores them
	 * for later viewing. These are mainly generated in CsvParsing with auto-detect enabled.
	 * If a parsing exception occurs, it will be caught and stored here. This returns whether
	 * any exceptions have occurred in data manipulation.
	 * @return true if the container has encountered any exceptions
	 */
	public final boolean hasExceptions() {
		return !exceptionLog.isEmpty();
	}
	
	public int hashCode() {
		int h = 0;
		for(DataColumn col : cols)
			h += col.hashCode();
		return h;
	}
	
	/**
	 * Finds and returns the index of a particular column
	 * @param col
	 * @return the index of a particular column
	 */
	public final int indexOf(DataColumn col) {
		return cols.indexOf(col);
	}
	
	/**
	 * Finds and returns the index of a particular row
	 * @param row
	 * @return the index of a particular row
	 */
	public final int indexOf(DataRow row) {
		return rows.indexOf(row);
	}
	
	/**
	 * Returns whether the table is empty
	 * @return false if the table contains data, true otherwise
	 */
	public boolean isEmpty() {
		return cols.isEmpty() && rows.isEmpty();
	}
	
	protected final boolean isRendered() {
		return isRendered;
	}
	
	/**
	 * Used to log exceptions thrown in CsvParsing generally. If any
	 * parse exceptions are thrown, they will be stored in this collection
	 * @param e
	 */
	public final void logException(Exception e) {
		exceptionLog.add(e);
	}
	
	/**
	 * Return the number of columns in the table
	 * @return number of cols in the table
	 */
	public final int ncol() {
		return cols.size();
	}
	
	/**
	 * Return the number of rows in the table
	 * @return number of rows in the table
	 */
	public final int nrow() {
		return rows.size();
	}
	
	public final HashMap<String,Integer> options() {
		return options;
	}
	
	/**
	 * Remove the column at the specified index
	 * @param arg0
	 * @return the removed DataColumn
	 */
	public final DataColumn removeColumn(int arg0) {
		if( !(cols.size() > arg0) )
			throw new NullPointerException();
		if( cols.size() == 1 ) {
			DataColumn c = cols.get(arg0);
			this.clear();
			return c;
		}
		
		for(int i = 0; i < rows.size(); i++)
			rows.get(i).remove(arg0);
		updateSchemaFromRemove(arg0);
		return cols.remove(arg0);
	}
	
	/**
	 * Remove the DataRow at the specified index
	 * @param arg0
	 * @return the removed DataRow
	 */
	public final DataRow removeRow(int arg0) {
		if( !(rows.size() > arg0) )
			throw new NullPointerException();
		if( rows.size() == 1 ) {
			DataRow r = rows.get(arg0);
			this.clear();
			return r;
		}

		for(int i = 0; i < cols.size(); i++)
			cols.get(i).remove(arg0);
		return rows.remove(arg0);
	}
	
	/**
	 * DataRows are susceptible to attaining schemas containing TheoreticalValues.
	 * The SchemaSafeDataStructure class can correct for this and identify the true schema if the data exists in columns,
	 * but the individual schemas may persist. This method will apply the true schema to each
	 * DataRow to eliminate TheoreticalValues from them and clean up the overall schema integrity
	 * of the table. It is recommended that after large adds, removals, transformations, etc, this
	 * method be called to create a constant schema throughout, however, this was designed
	 * as a non-essential method because the presence of TheoreticalValues in the individual
	 * DataRow schemas is not an issue in the table determining the proper schema.
	 */
	public final void renderState() {
		if(isRendered)
			return;
		for(DataRow row : rows)
			row.setSchema(schema);
		isRendered = true;
	}
	
	/**
	 * Return a collection of all the row names
	 * @return collection of the container's row names
	 */
	public Collection<String> rowNames() {
		Collection<String> names = new LinkedList<String>();
		for( DataRow r : rows ) {
			names.add(r.name);
		}
		return names;
	}
	
	/**
	 * Return a collection of the container's rows
	 * @return collection of the container's rows
	 */
	public final ArrayList<DataRow> rows() {
		return rows;
	}
	
	/**
	 * Will attempt to set a cell to a given object. If successful,
	 * will return the old object, otherwise will throw a SchemaMismatchException
	 * @param rowIndex - the row affected
	 * @param colIndex - the column affected
	 * @param o - the new Object
	 * @return the old Object
	 */
	@SuppressWarnings("unchecked")
	public final Object set(int rowIndex, int colIndex, Object o) {
		Object old;
		old = cols.get(colIndex).set(rowIndex, o);
		rows.get(rowIndex).set(colIndex, o);
		return old;
	}
	
	/**
	 * If returns zero, indicates an unsuccessful set. For a list of 
	 * settable options, see the method: getOptionKeys() or use the addOption()
	 * method to create a new option
	 * @param option
	 * @param num
	 * @return the set option parameter or zero for failure
	 */
	public final Integer setOptions(String option, Integer num) {
		if(!options.containsKey(option))
			return 0;
		else if(num < 1)
			return 0;
		else return options.put(option, num);
	}
	
	/**
	 * Replace a datacolumn at a given index
	 * @param index
	 * @param newCol
	 * @return the removed/old column at the specified index
	 */
	public final DataColumn setColumn(int index, DataColumn newCol) {
		if(newCol.isEmpty() || newCol.size() != this.nrow())
			throw new DimensionMismatchException();
		
		DataColumn old = this.removeColumn(index);
		this.addColumn(index, newCol);
		return old;
	}
	
	/**
	 * Replace a datarow at a given index
	 * @param index
	 * @param newRow
	 * @return the removed/old row from the specified index
	 */
	public final DataRow setRow(int index, DataRow newRow) {
		if(newRow.isEmpty() || newRow.size() != this.ncol())
			throw new DimensionMismatchException();
		
		DataRow old = this.removeRow(index);
		this.addRow(index, newRow);
		return old;
	}
	
	/**
	 * Set the respective column names given a List of Strings.
	 * The method will throw an exception for a list longer than 
	 * the number of columns, but will not fail if the provided
	 * list is shorter than the number of columns; it will merely
	 * not name the omitted columns
	 * @param names
	 */
	public void setColNames(List<String> names) {
		if(names.size() > cols.size())
			throw new IllegalArgumentException();
		else if(names.isEmpty() || names.size()==0)
			return;
		else {
			for(int i = 0; i < names.size(); i++)
				cols.get(i).setName(names.get(i));
		}
	}
	
	protected final void setRenderedState(boolean b) {
		isRendered = b;
	}
	
	/**
	 * Return the container's schema
	 */
	public final Schema schema() {
		return schema;
	}
	
	/**
	 * Determines whether the schema of the incoming row
	 * is safe to add to the current container
	 * @param sch
	 * @return whether an addition can be made
	 */
	protected final boolean schemaIsSafe(Schema sch) {
		return schema.isSafe(sch);
	}
	
	/**
	 * Resets any caches on add operations that may alter the state
	 * of the table
	 */
	protected final void tableUpdate() {
		isRendered = false;
	}
	
	/**
	 * Will return the unique DataColumns in the container
	 * @return a LinkedHashSet of unique DataColumns
	 */
	public final LinkedHashSet<DataColumn> uniqueColumnSet() {
		return new LinkedHashSet<DataColumn>(cols);
	}
	
	/**
	 * Will return the unique DataRows in the container
	 * @return a LinkedHashSet of unique DataRows
	 */
	public final LinkedHashSet<DataRow> uniqueRowSet() {
		return new LinkedHashSet<DataRow>(rows);
	}
	
	protected final void updateSchema(Class<? extends Object> appendable) {
		schema.add(appendable);
	}
	
	protected final void updateSchemaAt(int index, Class<? extends Object> appendable) {
		schema.add(index, appendable);
	}
	
	protected final void updateSchemaFromNew(Class<? extends Object> appendable) {
		if(null == schema)
			schema = new Schema();
		schema.add(appendable);
	}
	
	protected final void updateSchemaFromRemove(int index) {
		schema.remove(index);
	}
}
