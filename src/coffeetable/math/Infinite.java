package coffeetable.math;

import java.io.Serializable;

import coffeetable.utils.InfinityException;
import coffeetable.utils.MissingValueException;

public class Infinite extends TheoreticalValue implements Comparable<Number>, Serializable {
	private static final long serialVersionUID = 4527336245481667931L;
	private final int posOrNeg;
	private final String rep;
	
	public Infinite() {
		posOrNeg = 1;
		rep = "Infinity";
	}
	
	public Infinite(String s) {
		if(MissingValue.isNA(s))
			throw new MissingValueException(s + " is not infinite, it is NA");
		else if(!s.contains("inf"))
			throw new InfinityException(s + " is not infinite");

		boolean neg = s.contains("-");
		if(neg) {
			s.replace("-", "");
			posOrNeg = -1;
		} else posOrNeg = 1;
		
		rep = (neg ? "-" : "") + "Infinity";
	}
	
	/**
	 * Will always sort high if positive, low otherwise
	 */
	public int compareTo(Number arg0) {
		return posOrNeg > 0 ? 1 : -1;
	}
	
	public double doubleValue() {
		return posOrNeg > 0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
	}
	
	public static boolean isInfinite(Object o) {
		String s = o.toString().toLowerCase();
		return s.equals("inf")
			|| s.equals("infinite")
			|| s.equals("infinity")
			|| (o instanceof Double 
				&& o.equals(Double.POSITIVE_INFINITY));
	}

	public float floatValue() {
		return posOrNeg > 0 ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
	}

	public int intValue() {
		return posOrNeg > 0 ? Integer.MAX_VALUE : Integer.MIN_VALUE;
	}

	public long longValue() {
		return posOrNeg > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
	}
	
	public int sortOrder() {
		return posOrNeg;
	}

	public String toString() {
		return rep;
	}
}
