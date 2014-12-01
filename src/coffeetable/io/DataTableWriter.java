package coffeetable.io;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import coffeetable.datastructures.DataRow;
import coffeetable.datastructures.DataTable;

public class DataTableWriter {
	private static List<String> prohibited = Arrays.asList(new String[] {"\b","\\","[","]","-"});
	private final static String defaultDelimiter = ",";
	private boolean headers = true;
	private boolean rownames = false;
	private DataTable dt;
	private File file;
	private String delimiter;
	private final static String linesep = System.getProperty("line.separator");
	
	public DataTableWriter(DataTable dt, File file) {
		this.dt = dt;
		this.file = file;
		this.delimiter = defaultDelimiter;
		if(!fileAcceptable(file))
			throw new IllegalArgumentException("Unacceptable file");
	}
	
	public DataTableWriter(DataTable dt, File file, String delimiter, boolean headers, boolean rownames) {
		this(dt, file);
		this.delimiter = delimiterAcceptable(delimiter) ? delimiter : defaultDelimiter;
		this.headers = headers;
		this.rownames = rownames;
	}
	
	protected final static boolean delimiterAcceptable(String delimiter) {
		if(prohibited.contains(delimiter) || null==delimiter || delimiter.length() == 0)
			throw new IllegalArgumentException("Illegal delimiter");
		return true;
	}
	
	public DataTable dt() {
		return dt;
	}
	
	public File file() {
		return file;
	}
	
	private final static boolean fileAcceptable(File file) {
		return !file.isDirectory();
	}
	
	/**
	 * A method to be called from sub classes who may need to amend
	 * the extension
	 * @param file
	 */
	protected void setFile(File file) {
		this.file = file;
	}
	
	public boolean write() throws IOException {
		FileWriter f = null;
		try {
			f = new FileWriter(file);
			int rowCount = 0;
			int countIter = 0;
			ArrayList<DataRow> rows = new ArrayList<DataRow>(dt.rows());
			if(rownames) f.append("--" + delimiter); //Top left corner
			for(DataRow row : rows) {
				if( (0 == rowCount++) && headers ) {
					ArrayList<String> head = new ArrayList<String>(dt.columnNames());
					for(String o : head) {
						f.append(o);
						if(++countIter == head.size())
							f.append(linesep);
						else f.append(delimiter);
					}
					countIter = 0;
				}
				
				if(rownames) f.append(row.name() + delimiter);
				for(Object o : row) {
					f.append(o.toString());
					if(++countIter == row.size())
						f.append(linesep);
					else f.append(delimiter);
				}
				countIter = 0;
			}
		} catch (IOException e) {
			throw e;
		} finally {
			if(!(f==null)) {
				try {
					f.close();
				} catch (IOException e) {
					throw e;
				}
			}
		}
		return file.exists();
	}
}
