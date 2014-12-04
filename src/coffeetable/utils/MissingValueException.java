package coffeetable.utils;

/**
 * An exception class thrown when an illegal operation is performed 
 * on a MissingValue -- acts as a specific, super NullPointerException
 * @author Taylor G Smith
 */
public class MissingValueException extends RuntimeException {
	private static final long serialVersionUID = -8714063648685158447L;
	
	public MissingValueException() {
		super();
	}
	
	public MissingValueException(String s) {
		super(s);
	}
	
	public MissingValueException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public MissingValueException(Throwable cause) {
		super(cause);
	}
}
