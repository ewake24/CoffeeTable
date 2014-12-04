package coffeetable.math;

import java.io.Serializable;

/**
 * A super, abstract class to be extended by MissingValue, Infinite and NaN classes
 * @author Taylor G Smith
 */
public abstract class TheoreticalValue extends Number implements Comparable<Number>, Serializable {
	private static final long serialVersionUID = -2114691753319679398L;

	@Override
	public int compareTo(Number arg0) {
		return 0;
	}

	@Override
	public double doubleValue() {
		return 0;
	}

	@Override
	public float floatValue() {
		return 0;
	}

	@Override
	public int intValue() {
		return 0;
	}
	
	public static boolean isTheoretical(Object s) {
		return s.getClass().equals(TheoreticalValue.class)
			|| MissingValue.isNA(s)
			|| Infinite.isInfinite(s);
	}

	@Override
	public long longValue() {
		return 0;
	}
}