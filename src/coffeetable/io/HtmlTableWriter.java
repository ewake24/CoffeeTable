package coffeetable.io;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import coffeetable.datastructures.DataRow;
import coffeetable.datastructures.DataTable;

/**
 * A DataTable utility to write a DataTable to a simple HTML table. Can
 * be used if writing web documents in JSP.NET settings, where a Java
 * backend is used; a DataTable could be written from serverside to HTML on 
 * client side.
 * @author Taylor G Smith
 */
public class HtmlTableWriter extends DataTableWriter {
	private String tableClass = "table";
	private final static String closeTags="</table>";
	private String openTags = null;
	
	private final static String closeRowTags = "</tr>";
	private final static String openRowTags = "<tr>";
	private final static String closeHeaderTags = "</th>";
	private final static String openHeaderTags = "<th>";
	private final static String closeDataTags = "</td>";
	private final static String openDataTags = "<td>";
	
	private final static String tab = "\t";
	private final String newLine = System.getProperty("line.separator");
	private String caption;
	
	public HtmlTableWriter(DataTable dt, File file) {
		super(dt, file);
		openTags = "<table "+classString(tableClass)+">";
		caption = dt.name();
		
		if(!checkFile()) //Must end in .html
			super.setFile(new File(file.toString()+".html"));
	}
	
	private boolean checkFile() {
		return super.file().toString().endsWith(".html");
	}
	
	private String classString(String s) {
		return "class=\""+s+"\"";
	}
	
	public void setCaption(String arg0) {
		caption = (null == arg0 || arg0.isEmpty()) ? super.dt().name() : arg0;
	}

	public void setTableClass(String arg0) {
		if(null == arg0 || arg0.isEmpty()) {
			return;
		}
		tableClass = arg0.replace("\"", "").replace("'","").replace("<|>", "");
		openTags = "<table "+classString(tableClass)+">";
	}
	
	private String tableBuilder() {
		StringBuilder sb = new StringBuilder();
		sb.append(openTags+newLine);
		sb.append(tab+caption+newLine);
		
		/* Column headers for table */
		sb.append(tab+openRowTags+newLine);
		ArrayList<String> names = new ArrayList<String>(super.dt().columnNames());
		for(String name : names)
			sb.append(tab+tab+openHeaderTags+name+closeHeaderTags+newLine);
		sb.append(tab+closeRowTags+newLine+newLine);
		
		/* Data in each row */
		ArrayList<DataRow> rows = new ArrayList<DataRow>(super.dt().rows());
		for(DataRow row : rows ) {
			sb.append(tab+openRowTags+newLine);
			for(Object o : row)
				sb.append(tab+tab+openDataTags+o.toString()+closeDataTags+newLine);
			sb.append(tab+closeRowTags+newLine);
		}
		
		sb.append(closeTags);
		return sb.toString();
	}
	
	/**
	 * Will write a DataTable to a .html file in HTML table format
	 */
	public boolean write() throws IOException {
		FileWriter f = new FileWriter(super.file());
		try {
			f.write(tableBuilder());
		} catch(IOException e) {
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
		return super.file().exists();
	}
}
