package coffeetable.datastructures;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.TreeMap;


public abstract class RenderableSchemaSafeDataStructure extends SchemaSafeDataStructure implements java.io.Serializable {
	private static final long serialVersionUID = 1363057954945899788L;
	private String tableName;
	
	{
		tableName = "New Table";
	}
	
	public RenderableSchemaSafeDataStructure() { 
		super(); 
		setOptions();
	}
	
	public RenderableSchemaSafeDataStructure(int numRows, int numCols) {
		super(numRows, numCols);
		setOptions();
	}

	private final void setOptions() {
		super.addOption("max.print", 10000);	//What point the print-to-console cuts off
		super.addOption("col.whitespace", 4);	//Space between columns
		super.addOption("default.head", 6);		//Num rows in default printHead()
		super.addOption("print.col.names",1);
		super.addOption("print.table.name", 1);
	}
	
	/**
	 * Returns the name of the DataTable
	 */
	public String name() {
		return tableName;
	}

	/**
	 * Render the DataTable in the console
	 */
	public final void print() {
		printHead(rows().size());
	}
	
	/**
	 * Print the first n rows of the DataTable where n
	 * is defined by getOption("default.head")
	 */
	public final void printHead() {
		printHead( options().get("default.head") );
	}
	
	/**
	 * Print the first n rows of the DataTable where n
	 * is defined by the parameter, rows
	 * @param rows - the number of rows to render
	 */
	public final void printHead(int rows) {
		if(rows > this.rows().size())
			rows = this.rows().size();
		printTable(rows);
	}
	
	/**
	 * Determine the number of spaces to place between each item
	 * @param colToWidth
	 * @param content
	 * @param col
	 * @param whitespace
	 * @return
	 */
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
		int colWhitespace = options().get("col.whitespace");
		int maxPrint = options().get("max.print");
		boolean printname = options().get("print.table.name") == 1;
		boolean printcolnames = options().get("print.col.names") == 1;
		
		/* First generate the HashMap storing the column to the width
		 * Col number : col width */
		TreeMap<Integer, Integer> colToWidth = new TreeMap<Integer, Integer>();
		int index = 0;
		for( @SuppressWarnings("rawtypes") DataColumn column : columns() )
			colToWidth.put( index++, column.width() );
	
		/* Table/column names: */
		if(printname)
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
		if(printcolnames)
			System.out.println(names.toString());
		
		/* Rows: */
		int rowCount = 0;
		for( int row = 0; row < rows; row++ ) {
			DataRow d = this.rows().get(row);
			if(rowCount++ == maxPrint) {
				System.out.println(" [ reached getOption(\"max.print\") -- omitted " + (this.rows().size()-maxPrint) + " rows ]");
				break;
			}
			
			StringBuilder sb = new StringBuilder();
			for( int col = 0; col < d.size(); col++ ) {
				String content = d.get(col).toString();
				sb.append(printerHelper(colToWidth, content, col, colWhitespace));
			}
			System.out.println(sb.toString());
		}
		System.out.println();
	}
	
	/**
	 * Set the name of the DataTable. Note: <tt>null</tt> or empty
	 * Strings are not acceptable names
	 */
	public void setName(String name) {
		tableName = (null == name || name.isEmpty()) ? "New Table" : name; 
	}
}
