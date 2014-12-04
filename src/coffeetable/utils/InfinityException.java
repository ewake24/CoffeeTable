package coffeetable.utils;

/**
 * An exception class thrown when an illegal operation is performed 
 * on an Infinite value -- acts as a specific, super NullPointerException
 * @author Taylor G Smith
 */
public class InfinityException extends RuntimeException {
	private static final long serialVersionUID = -8714063648685158447L;
	
	public InfinityException() {
		super();
	}
	
	public InfinityException(String s) {
		super(s);
	}
	
	public InfinityException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public InfinityException(Throwable cause) {
		super(cause);
	}
}
