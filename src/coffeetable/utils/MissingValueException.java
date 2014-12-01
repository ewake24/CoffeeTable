package coffeetable.utils;

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
