package coffeetable.utils;

public class SchemaMismatchException extends RuntimeException {
	private static final long serialVersionUID = 8065584912510719748L;
	
	public SchemaMismatchException() {
		super();
	}
	
	public SchemaMismatchException(String s) {
		super(s);
	}
	
	public SchemaMismatchException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public SchemaMismatchException(Throwable cause) {
		super(cause);
	}
}
