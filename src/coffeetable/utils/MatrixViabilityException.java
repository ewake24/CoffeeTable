package coffeetable.utils;

/**
 * An exception class thrown when an illegal operation is performed 
 * on a Matrix -- whether it be dimensional mismatches in multiplication
 * or attempted adds of a non-numeric nature, this RuntimeException
 * alerts the user to the illegal operation.
 * 
 * @author Taylor G Smith
 */
public class MatrixViabilityException extends RuntimeException {
	private static final long serialVersionUID = -8709140648685158447L;
	
	public MatrixViabilityException() {
		super();
	}
	
	public MatrixViabilityException(String s) {
		super(s);
	}
	
	public MatrixViabilityException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public MatrixViabilityException(Throwable cause) {
		super(cause);
	}
}
