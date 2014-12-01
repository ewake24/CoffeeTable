package coffeetable.utils;

public class DimensionMismatchException extends RuntimeException {
	private static final long serialVersionUID = 8065584912510713648L;
	
	public DimensionMismatchException() {
		super();
	}
	
	public DimensionMismatchException(String s) {
		super(s);
	}
	
	public DimensionMismatchException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public DimensionMismatchException(Throwable cause) {
		super(cause);
	}
}
