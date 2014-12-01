package coffeetable.utils;

/**
 * Thrown for CsvParsing errors
 * @author Taylor G Smith
 */
public class CsvParseException extends RuntimeException {
	private static final long serialVersionUID = 8065905812510713648L;
	
	public CsvParseException() {
		super();
	}
	
	public CsvParseException(String s) {
		super(s);
	}
	
	public CsvParseException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public CsvParseException(Throwable cause) {
		super(cause);
	}
}