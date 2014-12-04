package coffeetable.datastructures;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * A class to deserialize a simple RJSONIO json data.frame to an instance
 * of DataTable. To do this, simply write your data.frame from R like this:
 * <tt>write(toJSON(data),"file_name.json")</tt>.
 * 
 * Alternatively, you may write a .txt or .csv and parse the data using the 
 * <tt>CsvParser</tt> class.
 * 
 * NOTE: CURRENTLY MARKED DEPRECATED UNTIL OUT OF ALPHA MODE
 * @author Taylor G Smith
 * @deprecated
 */
public class DeserializR {
	private static final Pattern p = Pattern.compile("[ \" /\\\\ \\} \\{ ]");
	private final File file;
	private final boolean autobox;
	
	public DeserializR(File file, boolean autodetectTypes) {
		if(!checkFile(file))
			throw new IllegalArgumentException("Bad file");
		else if(!file.getAbsolutePath().endsWith(".json"))
			throw new IllegalArgumentException("File must end in .json");
		this.file = file;
		autobox = autodetectTypes;
	}
	
	private boolean checkFile(File file) {
		return file.exists() && file.canRead();
	}
	
	/**
	 * Deserialize an R json object and create an instance of 
	 * DataTable from it.
	 * @return a new deserialized DataTable
	 */
	public DataTable deserializeJson() {
		String json = null;
		try {
			json = rawJson();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(null == json) throw new NullPointerException("Json is null");
		String[] fields = json.split(" ");
		DataTable r = new DataTable(file.getName());
		
		for(String s : fields) {
			String[] f = s.split(":");
			DataColumn<String> dc = new DataColumn<String>(f[0]); //With name
			String[] content = f[1].replace("[","").replace("]", "").split(",");
			ArrayList<String> arr = new ArrayList<String>(Arrays.asList(content));
			dc.addAll(arr);
			
			if(autobox && dc.isConvertableToNumeric()) {
				try {
					r.addColumn(dc.asNumeric());
				} catch (Exception e) {
					r.logException(e);
				}
			} else r.addColumn(dc);
		}
		
		return r;
	}
	
	private String rawJson() throws IOException {
		BufferedReader bf = new BufferedReader(new FileReader(file));
		StringBuilder sb = new StringBuilder();
		
		String line;
		while((line = bf.readLine()) != null)
			sb.append(line);
		bf.close();
		
		String output = p.matcher(sb.toString()).replaceAll("").replace(" ", "");
		output = output.replace("],", "] ");
		return output;
	}
}
