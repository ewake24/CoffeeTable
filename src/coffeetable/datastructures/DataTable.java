package coffeetable.datastructures;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import coffeetable.io.DataTableWriter;
import coffeetable.io.HtmlTableWriter;
import coffeetable.utils.DimensionMismatchException;
import coffeetable.utils.SchemaMismatchException;


/**
 * A collection of DataColumns and DataRows. This data structure
 * is designed to behave similar to an R dataframe or C# DataTable.  As in R, it provides the
 * ability to alter column classes, transform the matrix and perform vector operations on the columns.
 * 
 * NOTE: missing values should be represented as such (use the provided class 
 * MissingValue). Use of <tt>null</tt> will throw a NullPointerException for various
 * operations.
 * 
 * @author Taylor G Smith
 * @see DataColumn
 * @see DataRow
 * @see MissingValue
 */
@SuppressWarnings("rawtypes")
public class DataTable implements Serializable, Cloneable, VectorUtilities, RowUtilities {
	private static final long serialVersionUID = -246560507184440061L;
	private List<DataColumn> cols;
	private List<DataRow> rows;
	private String tableName;
	private LinkedList<Class<? extends Object>> schema;
	private ArrayList<Exception> exceptionLog;
	private HashMap<String, Integer> options;
	
	{
		schema = null;
		exceptionLog = new ArrayList<Exception>();
		tableName = "New Table";
		options = Options.setOptions();
	}
	
	public DataTable() {
		/* Init methods */
		initFromDefault();
	}
	
	public DataTable(String name) {
		this();
		setName(name);
	}
	
	public DataTable(Collection<DataRow> rows) {
		this(rows, null);
	}
	
	public DataTable(Collection<DataRow> rows, String name) {
		this(rows.size(), new ArrayList<DataRow>(rows).get(0).size(), name);
		addAllRows(rows);
	}
	
	/**
	 * A constructor to be called from the CsvParser class. Since the 
	 * parser can only determine the number of columns from the BufferedReader,
	 * it can pass that argument and speed up the instantiation of the class.
	 * @param cols
	 */
	protected DataTable(int cols) {
		if(cols < 0)
			throw new IllegalArgumentException("Cannot instantiate DataTable of negative dimensions");
		initLists( this.getOption("default.num.rows"),
				   this.setOptions("default.num.cols", cols) );
	}
	
	public DataTable(int rows, int cols) {
		this(rows,cols,null);
	}
	
	public DataTable(int rows, int cols, String name) {
		if(rows <= 0 || cols <= 0)
			throw new IllegalArgumentException("Cannot instantiate DataTable of zero or negative dimensions");
		initLists( this.setOptions("default.num.rows", rows),
				   this.setOptions("default.num.cols", cols) );
		setName(name);
	}
	
	/*--------------------------------------------------------------------*/
	/** A set of initialization methods that allow flexibility in the
	 * size of initialized lists for cols/rows.
	 */
	private void initFromDefault() {
		initLists(options.get("default.num.rows"),
				  options.get("default.num.cols"));
	}
	
	private void initLists(int r, int c) {
		cols = new ArrayList<DataColumn>(r);
		rows = new ArrayList<DataRow>(c);
	}
	/*--------------------------------------------------------------------*/
	
	/**
	 * Class for identifying the highest common convertable numeric type for
	 * DataColumns when an instance of DataTable undergoes a transformation.
	 * If a non-numeric column is identified, String.class will be returned.
	 * @author Taylor G Smith
	 */
	private final static class NumericClassHierarchy {
		public static Class<?> highestCommonConvertableClass(Collection<DataColumn> coll) {
			if(coll.isEmpty())
				return null;
			HashSet<Class> classes = new HashSet<Class>();
			for(DataColumn c : coll) {
				if(c.isNumeric()) 
					classes.add(c.contentClass());
				else if( c.isConvertableToNumeric() ) 
					classes.add(c.numericConversionType());
				else return String.class;
			}
			
			if(classes.size()==1)
				return new ArrayList<Class>(classes).get(0);
			else if(classes.contains(Double.class))
				return Double.class;
			else return Integer.class;
		}
	}
	
	
	/**
	 * Options hashmap instantiator. Internal class to avoid a second
	 * option setter method in the class body (disambiguation)
	 * @author Taylor G Smith
	 */
	private static class Options {
		public static HashMap<String,Integer> setOptions() {
			HashMap<String, Integer> options = new HashMap<String,Integer>();
			options.put("max.print", 10000);	//What point the print-to-console cuts off
			options.put("col.whitespace", 4);	//Space between columns
			options.put("default.head", 6);		//Num rows in default printHead()
			options.put("default.num.rows",25);	//Num rows to instantiate cols with
			options.put("default.num.cols",10);	//Num cols to instantiate rows with
			return options;
		}
	}
	
	/**
	 * Private inner class to handle all datatable sorts
	 * @author Taylor G Smith
	 */
	final static class TableRowSorter {
		private static void refactor( DataTable table, List<Integer> sorted ) {
			List<DataRow> r = new ArrayList<DataRow>(table.rows);
			List<String> colNames = new ArrayList<String>(table.columnNames());
			table.rows.clear();
			table.cols.clear();
			for(Integer i : sorted)
				table.addRow( r.get(i) );
			table.setColNames(colNames);
		}
		
		/**
		 * If table comprises only of one column, this is the most efficient way to sort it
		 */
		private static void refactorTableFromSingleColumn(DataTable table, boolean ascending) {
			DataColumn tmp = table.cols.get(0);
			if(ascending)
				tmp.sortAscending();
			else tmp.sortDescending();
			
			table.rows.clear();
			table.cols.clear();
			table.addColumn(tmp);
		}
		
		static void sortRowsAscending(DataColumn arg0, DataTable table) {
			sort(table, arg0, true);
		}

		static void sortRowsDescending(DataColumn arg0, DataTable table) {
			sort(table, arg0, false);
		}
		
		@SuppressWarnings("unchecked")
		private static void sort(DataTable table, DataColumn col, boolean asc) {
			if(table.isEmpty())
				return;
			else if(table.cols.size() == 1) {
				refactorTableFromSingleColumn(table,asc);
				return;
			} else if(table.rows.size() == 1)
				return;
			
			List<Integer> orderToSort = asc ? col.sortedAscendingMapEntries() : col.sortedDescendingMapEntries();
			refactor(table,orderToSort);
		}
	}
	
	/**
	 * Inner class for the cleaner management of content -- adds, removals, etc.
	 * @author Taylor G Smith
	 */
	@SuppressWarnings("unchecked")
	final static class ContentFactory {
		private static void addEmptyColsWithInitPopulate(DataTable table, DataRow row, boolean trusted) {
			for( int i = 0; i < row.size(); i++ ) {
				table.cols.add( trusted ? 
						new DataColumn(true,table.options.get("default.num.rows")) : 
							new DataColumn( table.options.get("default.num.rows")) );
				if(trusted)
					table.cols.get(i).addFromTrusted((Comparable)row.get(i));
				else table.cols.get(i).add((Comparable)row.get(i));
			}
		}
		
		private static void addEmptyRowsWithInitPopulate(DataTable table, DataColumn col) {
			for( int i = 0; i < col.size(); i++ ) {
				table.rows.add(new DataRow(col.size()));
				table.rows.get(i).add(col.get(i));
			}
		}
		
		private static void addToExistingCols(DataTable table, DataRow row, boolean trusted) {
			dimensionCheckRow(row, table);
			for( int i = 0; i < row.size(); i++ ) {
				if(trusted)
					table.cols.get(i).addFromTrusted((Comparable) row.get(i));
				else table.cols.get(i).add((Comparable) row.get(i));
			}
		}
		
		private static void addToExistingColsAtIndex(DataTable table, DataRow row, boolean trusted, int index) {
			dimensionCheckRow(row, table);
			for( int i = 0; i < row.size(); i++ ) {
				if(trusted)
					table.cols.get(i).addFromTrusted((Comparable) row.get(i), index);
				else table.cols.get(i).add(index, (Comparable) row.get(i));
			}
		}
		
		private static void addToExistingRows(DataTable table, DataColumn col ) {
			dimensionCheckCol(col, table);
			for( int i = 0; i < col.size(); i++ )
				table.rows.get(i).add(col.get(i));
		}
		
		private static void addToExistingRowsAtIndex(DataTable table, DataColumn col, int index) {
			dimensionCheckCol(col, table);
			for( int i = 0; i < col.size(); i++ )
				table.rows.get(i).add(index,col.get(i));
		}
		
		private static void dimensionCheckCol(DataColumn col, DataTable table) {
			if(!(col.size() == table.nrow()))
				throw new DimensionMismatchException("Column length does not match number of rows");
		}
		
		private static void dimensionCheckRow(DataRow row, DataTable table) {
			if(!(row.size() == table.ncol()))
				throw new DimensionMismatchException("Row length does not match number of columns");
		}
		
		private static void schemaAssignment(DataTable table, DataRow row) {
			if(null == table.schema || table.rows.isEmpty())
				table.schema = row.schema();
		}
	}
	
	/**
	 * For use with the CsvParser class --
	 * this saves time by not checking schemas and
	 * everything is read in as a String schema
	 * @param row
	 */
	protected void addRowFromCsvParser(DataRow row) {
		ContentFactory.schemaAssignment(this, row);
		rows.add(row);
		if(cols.isEmpty())
			ContentFactory.addEmptyColsWithInitPopulate(this,row,true);
		else ContentFactory.addToExistingCols(this,row, true);
	}
	
	public void addAllColumns(Collection<DataColumn> columns) {
		for(DataColumn col : columns)
			this.addColumn(col);
	}
	
	public void addAllRows(Collection<DataRow> rows) {
		for(DataRow row : rows)
			this.addRow(row);
	}
	
	public void addRow(DataRow row) {
		ContentFactory.schemaAssignment(this,row);
		if(!schemaIsSafe(row.schema()))
			throw new SchemaMismatchException("Row schemas do not match");
		rows.add(row);
		if(cols.isEmpty())
			ContentFactory.addEmptyColsWithInitPopulate(this,row,false);
		else ContentFactory.addToExistingCols(this,row,false);
	}
	
	public void addRow(int index, DataRow row) {
		if(rows.isEmpty() || null == schema || index==rows.size()) {
			addRow(row); return;
		} else if(index > rows.size())
			throw new ConcurrentModificationException("Proposed index suggests non-concurrent row addition (would require rows of NA values)");
		else if(!schemaIsSafe(row.schema()))
			throw new SchemaMismatchException("Row schemas do not match");
		rows.add(index, row);
		ContentFactory.addToExistingColsAtIndex(this, row, false, index);
	}
	
	public void addColumn(DataColumn<?> col) {
		if(cols.isEmpty() || null==schema)
			schema = updateSchemaFromNew(col.contentClass());
		else 
			schema = updateSchema(col.contentClass());
		cols.add(col);
		if(rows.isEmpty()) 
			ContentFactory.addEmptyRowsWithInitPopulate(this, col);
		else ContentFactory.addToExistingRows(this, col);
	}
	
	public void addColumn(int index, DataColumn<?> col) {
		if(cols.isEmpty() || null==schema || index==cols.size()) {
			addColumn(col); return;
		} else if(index > cols.size()) {
			throw new ConcurrentModificationException("Proposed index suggests non-concurrent column addition (would require cols of NA values)");
		}
		schema = updateSchemaAt(index, col.contentClass());
		cols.add(index,col);
		ContentFactory.addToExistingRowsAtIndex(this, col, index);
	}
	
	public void clear() {
		cols = new ArrayList<DataColumn>();
		rows = new ArrayList<DataRow>();
		schema = null;
	}
	
	public Object clone() {
		DataTable clone = this;
		clone.options = options;
		return clone;
	}
	
	public boolean containsNA() {
		if(cols.isEmpty())
			return false;
		for(DataColumn c : cols) {
			if(c.containsNA())
				return true;
		}
		return false;
	}
	
	public int countMissingValues() {
		if(this.isEmpty())
			return 0;
		int sum = 0;
		for(DataColumn t : cols) {
			sum += t.countMissingValues();
		}
		return sum;
	}
	
	@SuppressWarnings("unchecked")
	public static DataColumn<? extends Number> castColumnAsNumeric(DataColumn col) {
		return (DataColumn<? extends Number>) col.asNumeric();
	}
	
	@SuppressWarnings("unchecked")
	public static DataColumn<String> castColumnAsString(DataColumn col) {
		return (DataColumn<String>) col.asCharacter();
	}
	
	public Collection<String> columnNames() {
		Collection<String> names = new LinkedList<String>();
		for( DataColumn r : cols ) {
			names.add(r.name());
		}
		return names;
	}
	
	public Collection<DataColumn> columns() {
		return cols;
	}
	
	/**
	 * Will convert the DataColumn to Double, if it can be parsed (will keep the
	 * Number type--i.e., Integer--if it is already numeric). This is
	 * not a static method; it is only for instance-contained DataColumns.
	 * For a static version of the method, use columnAsNumeric.
	 * @param col
	 */
	public void convertColumnToNumeric(DataColumn col) {
		DataColumn<? extends Number> target = castColumnAsNumeric(col);
		int colInd = indexOf(col);
		this.removeColumn(colInd);
		this.addColumn(colInd, target);
	}
	
	/**
	 * Will convert the DataColumn to String. This is
	 * not a static method; it is only for instance-contained DataColumns.
	 * For a static version of the method, use columnAsString.
	 * @param col
	 */
	public void convertColumnToString(DataColumn col) {
		DataColumn<String> target = castColumnAsString(col);
		int colInd = indexOf(col);
		this.removeColumn(colInd);
		this.addColumn(colInd, target);
	}
	
	/**
	 * Will convert all columns in the datatable to String type
	 * @return true if the conversion was properly completed
	 */
	public boolean convertTableToCharacter() {
		if(this.isEmpty())
			return false;
		ArrayList<DataColumn> oldCols = new ArrayList<DataColumn>(cols);
		ArrayList<String> rowNames = new ArrayList<String>(this.rowNames());
		this.clear();
		for(DataColumn c : oldCols) {
			this.addColumn( castColumnAsString(c) );
		}
		this.setRowNames(rowNames);
		return true;
	}
	
	/**
	 * Will attempt to convert all contents of the table to numeric.
	 * @return true if successful, false otherwise.
	 */
	public boolean convertTableToNumeric() {
		if(this.isEmpty())
			return false;
		Class<?> converter = NumericClassHierarchy.highestCommonConvertableClass(cols);
		if(converter.equals(String.class))
			return false;
		
		ArrayList<DataColumn> oldCols = new ArrayList<DataColumn>(cols);
		ArrayList<String> rowNames = new ArrayList<String>(this.rowNames());
		this.clear();
		for(DataColumn c : oldCols) {
			this.addColumn( castColumnAsNumeric(c) );
		}
		this.setRowNames(rowNames);
		return true;
	}
	
	/**
	 * Determine whether the dicing operation will be performed across all columns
	 * @param colStart
	 * @param colEnd
	 * @param inclusive
	 * @return whether the subset is just removing rows
	 */
	private boolean diceWithAllColumns(int colStart, int colEnd, boolean inclusive) {
		return (colStart==0 && colEnd==cols.size()-1 && inclusive);
	}
	
	/**
	 * Determine whether the dicing operation will be performed across all rows
	 * @param rowStart
	 * @param rowEnd
	 * @param inclusive
	 * @return whether the subset is just removing columns
	 */
	private boolean diceWithAllRows(int rowStart, int rowEnd, boolean inclusive) {
		return (rowStart==0 && rowEnd==rows.size()-1 && inclusive);
	}
	
	/**
	 * Dices the DataTable into a subset
	 * @param colStart
	 * @param colEnd
	 * @param rowStart
	 * @param rowEnd
	 * @param inclusive
	 * @return a copy of the current instance of DataTable diced at the given boundaries
	 */
	public DataTable dice(int colStart, int colEnd, int rowStart, int rowEnd, boolean inclusive) {
		DataTable newdata = new DataTable();
		newdata.rows = rows;
		newdata.cols = cols;
		newdata.schema = schema;
		newdata.tableName = tableName;
		newdata.options = options;
		
		boolean diceWithAllColumns = diceWithAllColumns(colStart, colEnd, inclusive);
		boolean diceWithAllRows = diceWithAllRows(rowStart, rowEnd, inclusive);
		
		if(diceWithAllColumns && diceWithAllRows)
			return newdata;
		
		//Cut rows
		if(!diceWithAllRows) {
			if(!(rowStart == 0)) {
				newdata.removeRowRange(0, rowStart - (inclusive?1:0), true);
				newdata.removeRowRange((inclusive?1:0) + rowEnd-rowStart, newdata.nrow()-1, true);
			} else {
				newdata.removeRowRange(rowEnd+(inclusive?1:0), newdata.nrow()-1, true);
			}
		}
		
		//Cut cols
		if(!diceWithAllColumns) {
			if(!(colStart==0)) {
				newdata.removeColumnRange(0, colStart-(inclusive?1:0), true);
				newdata.removeColumnRange((inclusive?1:0) + colEnd-colStart, newdata.ncol()-1, true);
			} else {
				newdata.removeColumnRange(colEnd+(inclusive?1:0), newdata.ncol()-1, inclusive);
			}
		}
		
		return newdata;
	}
	
	/**
	 * Returns the list of any exceptions that have been handled since the instantiation
	 * of the DataTable instance (generally parsing exceptions if auto-detect type is
	 * enabled in the CsvParser)
	 * @return an ArrayList of exceptions the DataTable has encountered
	 */
	public ArrayList<Exception> exceptionLog() {
		return exceptionLog;
	}
	
	/**
	 * @param index
	 * @return DataColumn at specified index
	 */
	public DataColumn<?> getColumn(int index) {
		return cols.get(index);
	}
	
	/**
	 * @param name
	 * @return DataColumn with specified name
	 */
	public DataColumn<?> getColumn(String name) {
		ArrayList<String> names = new ArrayList<String>(columnNames());
		return this.getColumn(names.indexOf(name));
	}
	
	/**
	 * @param option
	 * @return the option parameter with the given name (ex: "max.print")
	 */
	public Integer getOption(String option) {
		if(!options.containsKey(option))
			return null;
		else return options.get(option);
	}
	
	/**
	 * The DataTable is governed by a set of 'options,' i.e., "max.print,"
	 * etc. These affect the behavior of some features of the DataTable.
	 * This method returns all keys in the options HashMap; the setOption()
	 * method will allow setting of option parameters
	 * @return the DataTable's valid, settable options
	 */
	public Set<String> getOptionKeys() {
		return options.keySet();
	}
	
	/**
	 * @param index
	 * @return DataRow at specified index
	 */
	public DataRow getRow(int index) {
		return rows.get(index);
	}
	
	/**
	 * @param name
	 * @return DataRow with specified name
	 */
	public DataRow getRow(String name) {
		ArrayList<String> names = new ArrayList<String>(rowNames());
		return this.getRow(names.indexOf(name));
	}
	
	/**
	 * If the DataTable has encountered any conversion exceptions, the log stores them
	 * for later viewing. These are mainly generated in CsvParsing with auto-detect enabled.
	 * If a parsing exception occurs, it will be caught and stored here. This returns whether
	 * any exceptions have occurred in data manipulation.
	 * @return true if the DataTable has encountered any exceptions
	 */
	public boolean hasExceptions() {
		return !exceptionLog.isEmpty();
	}
	
	/**
	 * Finds and returns the index of a particular column
	 * @param col
	 * @return the index of a particular column
	 */
	public int indexOf(DataColumn col) {
		return cols.indexOf(col);
	}
	
	/**
	 * Finds and returns the index of a particular row
	 * @param row
	 * @return the index of a particular row
	 */
	public int indexOf(DataRow row) {
		return rows.indexOf(row);
	}
	
	private final boolean inRange_col(int bottom, int top, boolean inclusive) {
		return inclusive ? (bottom >= 0) && (top <= cols.size()-1) && (bottom < top) :
					(bottom > -1) && (top <= cols.size()) && (bottom < top) && (top-bottom>1);
	}
	
	private final boolean inRange_row(int bottom, int top, boolean inclusive) {
		return inclusive ? (bottom >= 0) && (top <= rows.size()-1) && (bottom < top) :
					(bottom > -1) && (top <= rows.size()) && (bottom < top) && (top-bottom>1);
	}
	
	public boolean isEmpty() {
		return cols.isEmpty() && rows.isEmpty();
	}
	
	/**
	 * Used to log exceptions thrown in CsvParsing generally. If any
	 * parse exceptions are thrown, they will be stored in this collection
	 * @param e
	 */
	protected final void logException(Exception e) {
		exceptionLog.add(e);
	}
	
	public String name() {
		return tableName;
	}
	
	/**
	 * Will parse a file to an instance DataTable. The CsvParser will detect
	 * numeric types if told to, but this operation may take slightly longer.
	 * @param file
	 * @param delimiter
	 * @param headers
	 * @param detectNumeric
	 * @return a new instance of DataTable
	 * @throws IOException
	 */
	public static DataTable newFromFile(File file, String delimiter, boolean headers, boolean detectNumeric) throws IOException {
		CsvParser csv = new CsvParser(file, delimiter, headers);
		if(!detectNumeric)
			csv.disableAutoboxing();
		csv.parse();
		return csv.dataTable();
	}
	
	public int ncol() {
		return cols.size();
	}
	
	public int nrow() {
		return rows.size();
	}
	
	public void print() {
		printHead(rows.size());
	}
	
	public void printHead() {
		printHead( options.get("default.head") );
	}
	
	public void printHead(int rows) {
		if(rows > this.rows.size())
			rows = this.rows.size();
		printTable(rows);
	}
	
	private final String printerHelper(TreeMap<Integer, Integer> colToWidth, String content, int col, int whitespace) {
		int columnWidth = colToWidth.get(col);		//The widest point of this column
		int desiredWidth= columnWidth + whitespace;	//The rendered size of this column
		int itemWidth = content.length();			//Length of the content
		int deficit = desiredWidth - itemWidth;		//The number of spaces to add
		
		if(deficit < 0)
			throw new NegativeArraySizeException("There is an issue with " +
					"the type conversion method. " + "Please contact the author " +
					"(see the Javadoc for contact info)");
		
		char[] repeat = new char[deficit];
		Arrays.fill(repeat, ' ');
		return content + new String(repeat);
	}
	
	private void printTable(int rows) {
		if(this.isEmpty()) {
			System.out.println(new String()); 
			return;
		}
		int colWhitespace = options.get("col.whitespace");
		int maxPrint = options.get("max.print");
		
		/* First generate the HashMap storing the column to the width
		 * Col number : col width */
		TreeMap<Integer, Integer> colToWidth = new TreeMap<Integer, Integer>();
		int index = 0;
		for( DataColumn column : cols )
			colToWidth.put( index++, column.width() );
	
		/* Table/column names: */
		System.out.println("# -- " + tableName + " -- #");
		LinkedList<String> colnames = new LinkedList<String>(columnNames());
		StringBuilder names = new StringBuilder();
		for(int i = 0; i < colnames.size(); i++) {
			String name = colnames.get(i);
			
			/* //Adds space for the length of the i appendage -- unimplemented for now
			if(name.equals("DataColumn")) {
				if(colWhitespace < 2) {
					colWhitespace += Integer.valueOf(colnames.size()).toString().length();
					this.setOptions("col.whitespace", colWhitespace);
				}
				name = name + (i+1);
			} */
			
			names.append(printerHelper(colToWidth, name, i, colWhitespace));
		}
		System.out.println(names.toString());
		
		/* Rows: */
		int rowCount = 0;
		for( int row = 0; row < rows; row++ ) {
			DataRow d = this.rows.get(row);
			if(rowCount++ == maxPrint) {
				System.out.println(" [ reached getOption(\"max.print\") -- omitted " + (this.rows.size()-maxPrint) + " rows ]");
				break;
			}
			
			StringBuilder sb = new StringBuilder();
			for( int col = 0; col < d.size(); col++ ) {
				String content = d.get(col).toString();
				sb.append(printerHelper(colToWidth, content, col, colWhitespace));
			}
			System.out.println(sb.toString());
		}
	}
	
	public DataColumn removeColumn(int arg0) {
		if( !(cols.size() > arg0) )
			throw new NullPointerException();
		if( cols.size() == 1 ) {
			DataColumn c = cols.get(arg0);
			this.clear();
			return c;
		}
		
		for(int i = 0; i < rows.size(); i++)
			rows.get(i).remove(arg0);
		schema = updateSchemaFromRemove(arg0);
		return cols.remove(arg0);
	}
	
	public final void removeColumnRange(int lo, int hi, boolean inclusive) {
		if(!inRange_col(lo,hi,inclusive))
			throw new IllegalArgumentException("Out of range");
		int colsToRemove = inclusive ? ((hi+1)-(lo+1))+1 : ((hi+1)-(lo+1))-1;
		int index = 0;
		while(index++ < colsToRemove) {
			removeColumn(inclusive ? lo : lo+1);
		}
	}
	
	public DataRow removeRow(int arg0) {
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
	
	public final void removeRowRange(int lo, int hi, boolean inclusive) {
		if(!inRange_row(lo,hi,inclusive))
			throw new IllegalArgumentException("Out of range");
		int rowsToRemove = inclusive ? ((hi+1)-(lo+1))+1 : ((hi+1)-(lo+1))-1;
		int index = 0;
		while(index++ < rowsToRemove) {
			removeRow(inclusive ? lo : lo+1);
		}
	}
	
	public Collection<String> rowNames() {
		Collection<String> names = new LinkedList<String>();
		for( DataRow r : rows ) {
			names.add(r.name());
		}
		return names;
	}
	
	public Collection<DataRow> rows() {
		return rows;
	}
	
	public LinkedList<Class<?>> schema() {
		return schema;
	}
	
	public boolean schemaIsNumeric() {
		if(null==schema)
			return false;
		for(Class c : schema) {
			if(!Number.class.isAssignableFrom(c))
				return false;
		}
		return true;
	}
	
	@SuppressWarnings("unchecked")
	private final boolean schemaIsSafe(LinkedList<Class<?>> sch) {
		if(!(schema.contains(MissingValue.class) || sch.contains(MissingValue.class)))
			return schema.equals(sch);
		if(sch.size() != schema.size())
			return false;
		HashMap<Integer, Class> postcheckSetter = new HashMap<Integer, Class>();
		//Check...
		for(int i = 0; i < sch.size(); i++) {
			Class<?> c = sch.get(i);
			if(c.equals(MissingValue.class))
				continue;
			else if(schema.get(i).equals(MissingValue.class)) {
				postcheckSetter.put(i, c);
				continue;
			} else if(!c.equals(schema.get(i))) {
				return false;
			}
		}
		
		//Amend schema if needed...
		if(!postcheckSetter.isEmpty()) {
			for(Integer key : postcheckSetter.keySet())
				schema.set(key, postcheckSetter.get(key));
		}
		return true;
	}
	
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
	
	public void setName(String name) {
		tableName = (null == name || name.isEmpty()) ? "New Table" : name; 
	}
	
	/**
	 * If returns zero, indicates an unsuccessful set. For a list of 
	 * settable options, see the method: getOptionKeys()
	 * @param option
	 * @param num
	 * @return the set option parameter or zero for failure
	 */
	public Integer setOptions(String option, Integer num) {
		if(!options.containsKey(option))
			return 0;
		else if(num < 1)
			return 0;
		else return options.put(option, num);
	}
	
	public void setRowNames(List<String> names) {
		if(names.size() > rows.size())
			throw new IllegalArgumentException();
		else if(names.isEmpty() || names.size()==0)
			return;
		else {
			for(int i = 0; i < names.size(); i++)
				rows.get(i).setName(names.get(i));
		}
	}
	
	/**
	 * Sort the entire DataTable in order of a DataColumn sorted ascending
	 * @param col
	 */
	public void sortAscending(DataColumn col) {
		if(!cols.contains(col))
			throw new IllegalArgumentException("Column does not exist");
		TableRowSorter.sortRowsAscending(col, this);
	}
	
	/**
	 * Sort the entire DataTable in order of a DataColumn sorted descending
	 * @param col
	 */
	public void sortDescending(DataColumn col) {
		if(!cols.contains(col))
			throw new IllegalArgumentException("Column does not exist");
		TableRowSorter.sortRowsDescending(col, this);
	}
	
	/**
	 * Matrix-transform the DataTable
	 */
	public void transform() {
		if(this.isEmpty())
			throw new NullPointerException();
		Class<?> converter = null;
		if(!(new HashSet<Class<? extends Object>>(schema).size()==1)) { //If not all the same class of Cols
			converter = NumericClassHierarchy.highestCommonConvertableClass(cols);
		} else converter = schema.get(0);
		
		List<DataRow> numericDCs = new ArrayList<DataRow>();
		List<String> newColNames = new ArrayList<String>(rowNames());
		for(DataColumn d : cols) {
			DataColumn dar = converter.equals(Double.class) ? d.asDouble() : 
								(converter.equals(Integer.class) ? d.asInteger() : 
									d.asCharacter());
			numericDCs.add(dar.toDataRow());
		}
		this.clear();
		for(DataRow d : numericDCs)
			addRow(d);
		//schema = numericDCs.get(0).schema(); //Don't need to set this, addRow() does
		this.setColNames(newColNames);
	}
	
	private final LinkedList<Class<? extends Object>> updateSchema(Class<? extends Object> appendable) {
		LinkedList<Class<? extends Object>> list = schema;
		list.add(appendable);
		return list;
	}
	
	private final LinkedList<Class<? extends Object>> updateSchemaAt(int index, Class<? extends Object> appendable) {
		LinkedList<Class<? extends Object>> list = schema;
		list.add(index, appendable);
		return list;
	}
	
	private final LinkedList<Class<? extends Object>> updateSchemaFromNew(Class<? extends Object> appendable) {
		LinkedList<Class<? extends Object>> list = new LinkedList<Class<? extends Object>>();
		list.add(appendable);
		return list;
	}
	
	private final LinkedList<Class<? extends Object>> updateSchemaFromRemove(int index) {
		LinkedList<Class<? extends Object>> list = schema;
		list.remove(index);
		return list;
	}
	
	/**
	 * Writes a delimited file (.csv, .txt, etc.)
	 * @param file
	 * @param delimiter
	 * @param headers
	 * @param rownames
	 * @return true if successful
	 */
	public boolean writeToFile(File file, String delimiter, boolean headers, boolean rownames) {
		DataTableWriter dtw = new DataTableWriter(this, file, delimiter, headers, rownames);
		try {
			return dtw.write();
		} catch (IOException e) {
			this.logException(e);
			return false;
		}
	}
	
	/**
	 * Writes to a default HTML table. The HtmlTableWriter class provides support for
	 * "fancy" writes that include class additions, but this method will write a default
	 * table. For a fancy write, instantiate an instance of HtmlTableWriter.
	 * @param file
	 * @return true if successful
	 */
	public boolean writeToHtml(File file) {
		HtmlTableWriter html = new HtmlTableWriter(this, file);
		try {
			return html.write();
		} catch (IOException e) {
			this.logException(e);
			return false;
		}
	}
}
