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
import java.util.List;

import coffeetable.datatypes.Factor;
import coffeetable.interfaces.RowUtilities;
import coffeetable.io.DataTableWriter;
import coffeetable.io.HtmlTableWriter;
import coffeetable.math.MissingValue;
import coffeetable.math.Infinite;

/**
 * A collection of DataColumns and DataRows. This data structure
 * is designed to behave similar to an R dataframe or C# DataTable.  As in R, it provides the
 * ability to alter column classes, transform the matrix, perform vector operations on the columns
 * and subset based on a SubsettableCondition class.
 * 
 * NOTE: missing values should be represented as such (use the provided class 
 * MissingValue). Use of <tt>null</tt> will throw a NullPointerException for various
 * operations.
 * 
 * @author Taylor G Smith
 * @see DataColumn
 * @see DataRow
 * @see MissingValue
 * @see Infinite
 * @see SchemaSafeDataStructure
 * @see SubsettableCondition
 */
@SuppressWarnings("rawtypes")
public class DataTable extends AbstractDataTable implements java.io.Serializable, Cloneable, RowUtilities {
	private static final long serialVersionUID = -246560507184440061L;
	
	/**
	 * Instantiate an empty DataTable
	 */
	public DataTable() {
		super();
	}
	
	/**
	 * Instantiate an empty DataTable with a name
	 * @param name
	 */
	public DataTable(String name) {
		this();
		setName(name);
	}
	
	/**
	 * Instantiate a DataTable and immediately populate it with
	 * a collection of DataRows
	 * @param rows
	 */
	public DataTable(Collection<DataRow> rows) {
		this(rows, null);
	}
	
	/**
	 * Instantiate a named DataTable and immediately 
	 * populate it with a collection of DataRows
	 * @param rows
	 * @param name
	 */
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
		super( defaultNumRows,cols );
		this.setOptions("default.num.cols", cols);
	}
	
	/**
	 * Instantiate an empty DataTable with pre-allocated dimensions.
	 * This can save time on population if the dimensions are known beforehand
	 * @param rows
	 * @param cols
	 */
	public DataTable(int rows, int cols) {
		this(rows,cols,null);
	}
	
	/**
	 * Instantiate an empty, named DataTable with pre-allocated dimensions.
	 * This can save time on population if the dimensions are known beforehand
	 * @param rows
	 * @param cols
	 * @param name
	 */
	public DataTable(int rows, int cols, String name) {
		super(rows,cols);
		this.setOptions("default.num.rows", rows);
		this.setOptions("default.num.cols", cols);
		setName(name);
	}
	
	/*--------------------------------------------------------------------*/
	
	/**
	 * Class for identifying the highest common convertable numeric type for
	 * DataColumns when an instance of DataTable undergoes a transformation.
	 * If a non-numeric column is identified, String.class will be returned.
	 * @author Taylor G Smith
	 */
	final static class NumericClassHierarchy {
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
	 * Private inner class to handle all datatable sorts by calling protected
	 * methods inside of DataColumn class. Works by sorting the defined column,
	 * receiving a Collection of Map.Entries of record : index, and the sorted order,
	 * then reinserting the rows in the sorted order
	 * @author Taylor G Smith
	 */
	final static class TableRowSorter {
		private static void refactor( DataTable table, List<Integer> sorted ) {
			List<DataRow> r = new ArrayList<DataRow>(table.rows());
			List<String> colNames = new ArrayList<String>(table.columnNames());
			table.rows().clear();
			table.columns().clear();
			for(Integer i : sorted)
				table.addRow( r.get(i) );
			table.setColNames(colNames);
		}
		
		/**
		 * If table comprises only of one column, this is the most efficient way to sort it
		 */
		private static void refactorTableFromSingleColumn(DataTable table, boolean ascending) {
			DataColumn tmp = table.columns().get(0);
			if(ascending)
				tmp.sortAscending();
			else tmp.sortDescending();
			
			table.rows().clear();
			table.columns().clear();
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
			else if(table.columns().size() == 1) {
				refactorTableFromSingleColumn(table,asc);
				return;
			} else if(table.rows().size() == 1)
				return;
			
			List<Integer> orderToSort = asc ? col.sortedAscendingMapEntries() : col.sortedDescendingMapEntries();
			refactor(table,orderToSort);
		}
	}
	
	/**
	 * For use with the CsvParser class --
	 * this saves time by not checking schemas and
	 * everything is read in as a String schema
	 * @param row
	 */
	protected final void addRowFromCsvParser(DataRow row) {
		ContentFactory.schemaAssignment(this, row);
		rows().add(row);
		if(columns().isEmpty())
			ContentFactory.addEmptyColsWithInitPopulate(this,row,true);
		else ContentFactory.addToExistingCols(this,row, true);
	}

	/**
	 * Creates a Factor version of the column passed in
	 * @param col
	 * @return a Factor version of the column
	 */
	@SuppressWarnings("unchecked")
	public final static DataColumn<Factor> castColumnAsFactor(DataColumn col) {
		return (DataColumn<Factor>) col.asFactor();
	}
	
	/**
	 * Creates a numeric version of the column passed in
	 * @param col
	 * @return a numeric version of the column
	 */
	@SuppressWarnings("unchecked")
	public final static DataColumn<? extends Number> castColumnAsNumeric(DataColumn col) {
		return (DataColumn<? extends Number>) col.asNumeric();
	}
	
	/**
	 * Casts the column to String
	 * @param col
	 * @return the passed in column in String version
	 */
	@SuppressWarnings("unchecked")
	public final static DataColumn<String> castColumnAsString(DataColumn col) {
		return (DataColumn<String>) col.asCharacter();
	}
	
	/**
	 * Will convert the DataColumn to Factor. This is not a static method;
	 * it is only for instance-contained DataColumns. For a static version
	 * of the method, user castColumnAsFactor
	 * @param col
	 */
	public final void convertColumnToFactor(DataColumn col) {
		DataColumn<Factor> target = castColumnAsFactor(col);
		this.setColumn(indexOf(col), target);
	}
	
	/**
	 * Will convert the DataColumn to Double, if it can be parsed (will keep the
	 * Number type--i.e., Integer--if it is already numeric). This is
	 * not a static method; it is only for instance-contained DataColumns.
	 * For a static version of the method, use castColumnAsNumeric.
	 * @param col
	 */
	public final void convertColumnToNumeric(DataColumn col) {
		DataColumn<? extends Number> target = castColumnAsNumeric(col);
		this.setColumn(indexOf(col), target);
	}
	
	/**
	 * Will convert the DataColumn to String. This is
	 * not a static method; it is only for instance-contained DataColumns.
	 * For a static version of the method, use castColumnAsString.
	 * @param col
	 */
	public final void convertColumnToString(DataColumn col) {
		DataColumn<String> target = castColumnAsString(col);
		this.setColumn(indexOf(col), target);
	}
	
	/**
	 * Will remove all rows containing any TheoreticalValue.class objects
	 * from a cloned instance of this table
	 */
	public final DataTable completeCases() {
		DataTable dt = (DataTable) this.clone();
		for(int i = 0; i < dt.rows().size(); i++) {
			if(i >= dt.rows().size())
				break;
			
			if(dt.rows().get(i).containsNA())
				dt.removeRow(i--);
		}
		return dt;
	}
	
	/**
	 * Will convert all columns in the datatable to String type
	 * @return true if the conversion was properly completed
	 */
	public final boolean convertTableToCharacter() {
		if(this.isEmpty())
			return false;
		ArrayList<DataColumn> oldCols = new ArrayList<DataColumn>(columns());
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
	public final boolean convertTableToNumeric() {
		if(this.isEmpty())
			return false;
		Class<?> converter = NumericClassHierarchy.highestCommonConvertableClass(columns());
		if(converter.equals(String.class))
			return false;
		
		ArrayList<DataColumn> oldCols = new ArrayList<DataColumn>(columns());
		ArrayList<String> rowNames = new ArrayList<String>(this.rowNames());
		this.clear();
		for(DataColumn c : oldCols) {
			this.addColumn( castColumnAsNumeric(c) );
		}
		this.setRowNames(rowNames);
		return true;
	}
	
	/**
	 * Clears all data from the DataTable, including the name
	 */
	public void clear() {
		super.clear();
		super.setName(null);
	}
	
	public Object clone() {
		DataTable clone = new DataTable(rows(), name());
		for(String key : this.options().keySet())
			clone.setOptions(key, this.options().get(key));
		for(Exception e : exceptionLog())
			clone.logException(e);
		clone.setRenderedState(super.isRendered());
		clone.setColNames(new ArrayList<String>(this.columnNames()));	
		return clone;
	}
	
	/**
	 * Dices the DataTable into a subset
	 * @param rowStart - row from which to begin subset (inclusive)
	 * @param rowEnd - row at which to end subset (inclusive)
	 * @param colStart - col at which to begin subset (inclusive)
	 * @param colEnd - col at which to end subset (inclusive)
	 * @return a copy of the current instance of DataTable diced at the given boundaries
	 */
	public final DataTable dice(int rowStart, int rowEnd, int colStart, int colEnd) {
		return (DataTable) super.dice(rowStart,rowEnd,colStart,colEnd);
	}
	
	public boolean equals(Object o) {
		if(!(o instanceof DataTable))
			return false;
		return o.hashCode() == this.hashCode();
	}
	
	public int hashCode() {
		return super.hashCode()^(int)DataTable.serialVersionUID;
	}
	
	/**
	 * Will parse a file to an instance DataTable. The CsvParser will detect
	 * numeric types if told to, but this operation may take slightly longer.
	 * It is recommended the parameter, 'renderstate', be set to true, as this
	 * will ensure accurate schemas for all DataRows and imposes only a very slight
	 * runtime overhead
	 * 
	 * @param file
	 * @param delimiter
	 * @param headers
	 * @param detectNumeric - whether to autodetect column types. If false, will return string
	 * @param renderstate - whether to correct NAs in row schemas with overall table schema (low-impact op; recommended)
	 * @return a new instance of DataTable
	 * @throws IOException
	 */
	public final static DataTable newFromFile(File file, String delimiter, boolean headers, 
			boolean detectNumeric, boolean renderstate) throws IOException {
		CsvParser csv = new CsvParser(file, delimiter, headers);
		if(!detectNumeric)
			csv.disableAutoboxing();
		if(!renderstate)
			csv.disableRenderState();
		csv.parse();
		return csv.dataTable();
	}
	
	public final static DataTable readFromSerializedObject(FileInputStream fileIn) throws IOException, ClassNotFoundException {
		DataTable d = null;
		ObjectInputStream in = new ObjectInputStream(fileIn);
		d = (DataTable) in.readObject();
		in.close();
		fileIn.close();
		return d;
	}
	
	/**
	 * Sort the entire DataTable in order of a DataColumn sorted ascending
	 * @param col
	 */
	public final void sortAscending(DataColumn col) {
		if(!columns().contains(col))
			throw new IllegalArgumentException("Column does not exist");
		TableRowSorter.sortRowsAscending(col, this);
	}
	
	/**
	 * Sort the entire DataTable in order of a DataColumn sorted descending
	 * @param col
	 */
	public final void sortDescending(DataColumn col) {
		if(!columns().contains(col))
			throw new IllegalArgumentException("Column does not exist");
		TableRowSorter.sortRowsDescending(col, this);
	}
	
	/**
	 * Will subset the datatable according to rows in a column that correspond
	 * to a given SubsettableCondition
	 * @param eval
	 * @param sub
	 * @return a subset DataTable that meets the conditions in the SubsettableCondition class
	 */
	public final DataTable subsetByCondition(DataColumn eval, SubsettableCondition sub) {
		if(!columns().contains(eval))
			throw new IllegalArgumentException("Specified column not found in table");
		boolean[] keeps = eval.subsetLogicalVector(sub);
		
		DataTable dt = new DataTable(rows(), name()+"_Subset");
		for(String key : this.options().keySet())
			dt.setOptions(key, this.options().get(key));
		
		dt.setColNames(new ArrayList<String>(this.columnNames()));
		dt.setRowNames(new ArrayList<String>(this.rowNames()));
		
		int j = 0;
		for(int i = 0; i < rows().size(); i++) {
			if(!keeps[i])
				dt.removeRow(j);
			else j++; //Only increments if true
		}
		
		dt.clearColumnCaches();
		return dt;
	}
	
	/**
	 * Matrix-transform the DataTable
	 */
	public final void transform() {
		if(this.isEmpty())
			throw new NullPointerException();
		Class<?> converter = null;
		if(!schema().isSingular()) { //If not all the same class of Cols
			converter = NumericClassHierarchy.highestCommonConvertableClass(columns());
		} else converter = schema().getContentClass();
		
		List<DataRow> numericDCs = new ArrayList<DataRow>();
		List<String> newColNames = new ArrayList<String>(rowNames());
		for(DataColumn d : columns()) {
			/* Need to assess here instead of in DataColumn control logic to ensure highest common
			 * convertable class (for table) is used, where DataColum will not look at all columns, 
			 * but only the one being converted */
			DataColumn dar = converter.equals(String.class) ? d.asCharacter() : 
								converter.equals(Double.class) ? d.asDouble() : d.asInteger();
			numericDCs.add(dar.toDataRow());
		}
		this.clear();
		this.addAllRows(numericDCs);
		this.setColNames(newColNames);
	}
	
	/**
	 * Create a new DataTable from the unique rows in the datastructure
	 * @return an instance of DataTable identical to the current instance but
	 * without any duplicate rows
	 */
	public DataTable uniqueRows() {
		DataTable dt = new DataTable(super.uniqueRowSet(), name());
		for(String key : this.options().keySet())
			dt.setOptions(key, this.options().get(key));
		
		dt.setRenderedState( this.isRendered() );
		for(Exception e : exceptionLog())
			dt.logException(e);
		
		return dt;
	}
	
	/* ---------- Table writing operations ---------- */
	/**
	 * Writes a serialized DataTable object from this instance,
	 * returns true if successful
	 * @throws IOException
	 * @param path - the path to which to write
	 * @return true if the operation was successful
	 */
	public final boolean writeObject(String path) throws IOException {
		if(null == path || path.isEmpty()) {
			path = "/tmp/datatable.ser"; 
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
	
	/**
	 * Writes a delimited file (.csv, .txt, etc.)
	 * @param file
	 * @param delimiter
	 * @param headers
	 * @param rownames
	 * @return true if successful
	 * @throws IOException 
	 */
	public final boolean writeToFile(File file, String delimiter, boolean headers, boolean rownames) throws IOException {
		DataTableWriter dtw = new DataTableWriter(this, file, delimiter, headers, rownames);
		return dtw.write();
	}
	
	/**
	 * Writes to a default HTML table. The HtmlTableWriter class provides support for
	 * "fancy" writes that include class additions, but this method will write a default
	 * table. For a fancy write, instantiate an instance of HtmlTableWriter and pass it
	 * as an arg
	 * @param file
	 * @return true if successful
	 * @throws IOException 
	 */
	public final boolean writeToHtml(File file) throws IOException {
		return writeToHtmlPrivate(new HtmlTableWriter(this,file));
	}
	
	/**
	 * Allows for "fancy" writes of an Html table with a custom HtmlTableWriter object
	 * @param html - HtmlTableWriter object
	 * @return true if successful
	 * @throws IOException 
	 */
	public final boolean writeToHtml(HtmlTableWriter html) throws IOException {
		html.setTable(this); //Ensure this table will be written
		return writeToHtmlPrivate(html);
	}
	
	private boolean writeToHtmlPrivate(HtmlTableWriter html) throws IOException {
		return html.write();
	}
}
