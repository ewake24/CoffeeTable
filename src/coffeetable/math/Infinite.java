package coffeetable.math;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;

import coffeetable.utils.InfinityException;

public class Infinite extends TheoreticalValue implements Comparable<Number>, Serializable {
	private static final long serialVersionUID = 4527336245481667931L;
	private final static HashSet<String> acceptable = new HashSet<String>(
		Arrays.asList( new String[] {"inf","infinite","infinity"} )
	);
	private final int posOrNeg;
	private final String rep;
	
	public Infinite() {
		posOrNeg = 1;
		rep = "Infinity";
	}
	
	public Infinite(String s) {
		if(null == s)
			throw new NullPointerException(s + " is not infinite");

		boolean neg = s.contains("-");
		if(neg) {
			s = s.replace("-", "");
			posOrNeg = -1;
		} else posOrNeg = 1;
		
		rep = (neg ? "-" : "") + "Infinity";
		if(!acceptable.contains(s.toLowerCase()))
			throw new InfinityException(s + " is not infinite!");
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
		return s.equals("inf") || s.equals("-inf")
			|| s.equals("infinite") || s.equals("-infinite")
			|| s.equals("infinity") || s.equals("-infinity")
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
