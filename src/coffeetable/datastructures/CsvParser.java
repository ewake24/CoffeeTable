package coffeetable.datastructures;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import coffeetable.utils.CsvParseException;

/**
 * A class designed to create a new DataTable from a file input. NOTE: if you get
 * and IllegalArgumentException attesting that there is a dimension mismatch in 
 * headers and content, ensure there are no stray delimiters in the data. This is most
 * common when parsing a CSV; occasionally a thousands-separator (1,000) will be left
 * in place and the parser will attempt to split this, resulting in too many columns
 * for the headers
 * @author Taylor G Smith
 * @see DataTable
 */
public final class CsvParser {
	private static final List<String> prohibited = Arrays.asList(new String[] {"\b","\\","[","]","-"}); //Prohibited delimiters
	private static final Pattern p = Pattern.compile("[\\t \" \\r \b \\n \\* \\p{C}]"); 			//Don't want spaces
	private static final Pattern headerPattern = Pattern.compile("[\\t\"\\r\b\\n\\*\\p{C}]"); //Spaces OK
	private final static String defaultDelimiter = ",";
	private boolean autobox = true;
	private boolean headers = false;
	private boolean renderstate = true;
	private String delimiter = null;
	private DataTable datatable;
	private File file;
	
	public CsvParser(File file, String delimiter, boolean headers) throws FileNotFoundException {
		if(!fileIsAcceptable(file))
			throw new FileNotFoundException();
		this.file = file;
		this.delimiter = delimiterAcceptable(delimiter) ? delimiter : defaultDelimiter;
		this.headers = headers;
	}
	
	public CsvParser(File file, String delimiter) throws FileNotFoundException {
		this(file, delimiter, false);
	}
	
	public CsvParser(File file) throws FileNotFoundException {
		this(file, defaultDelimiter);
	}
	
	public DataTable dataTable() {
		return datatable;
	}
	
	@SuppressWarnings("rawtypes")
	private void autoBox() {
		if(datatable.isEmpty())
			return;
		List<DataColumn> list = new ArrayList<DataColumn>(datatable.columns());
		for(DataColumn<String> col : list) {
			if(col.isConvertableToNumeric()) {
				try {
					datatable.convertColumnToNumeric(col);
				} catch(Exception e) {
					datatable.logException(e);
				}
			}
		}
	}
	
	private final String[] cleanArray(String[] arr, boolean headers) {
		String[] returnable = new String[arr.length];
		for(int i = 0; i < arr.length; i++) {
			String t = headers ? headerPattern.matcher(arr[i]).replaceAll("") 
					: p.matcher(arr[i]).replaceAll("");
			returnable[i] = t.equals("") ? "NA" : t;
		}
		return returnable;
	}
	
	protected final static boolean delimiterAcceptable(String delimiter) {
		if(prohibited.contains(delimiter) || null==delimiter || delimiter.length() == 0)
			throw new IllegalArgumentException("Illegal delimiter");
		return true;
	}
	
	public String delimiter() {
		return delimiter;
	}
	
	public void disableAutoboxing() {
		autobox = false;
	}
	
	public void disableRenderState() {
		renderstate = false;
	}
	
	private final static boolean fileIsAcceptable(File file) {
		return file.exists() && file.isFile() && file.canRead();
	}
	
	/**
	 * Will parse the file to an instance of DataTable. There is some
	 * overhead associated with this method as it cleans each data entry and 
	 * then autoboxes the results by trying to parse each as numeric if
	 * possible
	 * @throws IOException
	 */
	public void parse() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = "";
		
		int rowCount = 0;
		String[] headerlist = null;
		Schema schema = null;
		try {
			while( (line = br.readLine()) != null ) {
				if(!line.contains(delimiter)) {
					br.close();
					throw new IllegalArgumentException("Specified delimiter does not exist");
				} else if(++rowCount==1 && headers) {
					headerlist = cleanArray(line.split(delimiter),true);
					continue;
				} else if(line.endsWith(delimiter)) //Have to in order to avoid dim mismatch
					line += "NA";
				
				String[] row = cleanArray(line.split(delimiter),false);
				if(null == datatable)
					datatable = new DataTable(row.length); // Invoke protected constructor
				
				/* If line ends with delimiter, it is a missing value on the last column */
				if(headers && (row.length!=headerlist.length)) {
					br.close();
					throw new IllegalArgumentException("Dimension mismatch between headers and rows"
							+ Arrays.asList(row));
				}
				
				if(null == schema) {
					schema = new Schema();
					for(int i = 0; i < row.length; i++)
						schema.add(String.class);
				}
				DataRow R = new DataRow(Arrays.asList(row));
				R.setSchema(schema);
				datatable.addRowFromCsvParser(R);
			}
		} catch(CsvParseException e) {
			throw e;
		} finally {
			br.close();
		}
		if(headers && !(headerlist.length == 0))
			datatable.setColNames( Arrays.asList(headerlist) );
		if(autobox)
			autoBox();
		if(renderstate)
			datatable.renderState();
	}
}
