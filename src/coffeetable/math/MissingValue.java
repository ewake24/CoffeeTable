package coffeetable.math;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * To be used in conjunction with DataTable in place of a <tt>null</tt> value.
 * MissingValue is a comparable class (both in its use as a null value as well as its
 * interface implementation) that can be sorted, treated as either numeric or 
 * String type and otherwise ignored.  Its Number value is that of <tt>Integer.MIN_VALUE</tt>
 * and will always sort low, unless a Comparator reverses the order.
 * @author Taylor G Smith
 * @see Number
 */
public final class MissingValue extends TheoreticalValue implements Comparable<Number>, Serializable {
	private static final long serialVersionUID = 8685839276702330957L;
	private static final Pattern pattern = Pattern.compile("[\\< \\> \\/]");
	private static Integer value = Integer.MIN_VALUE;
	private static int sortOrder = -1; //future implementation to allow toggle
	private String rep = "NA";
	
	public MissingValue() {
		/*Any future inits*/
	}

	/**
	 * Will always sort low
	 */
	public int compareTo(Number arg0) {
		return sortOrder;
	}
	
	public double doubleValue() {
		return value;
	}
	
	public static boolean isNA(Object o) {
		if(null == o || o.getClass().equals(MissingValue.class))
			return true;
		String s = o.toString();
		if(s.isEmpty())
			return true;
		s = pattern.matcher(s).replaceAll("").toLowerCase();
		return s.equals("na");
	}
	
	public boolean equals(Object arg0) {
		return (arg0 instanceof MissingValue) && arg0.hashCode() == this.hashCode();
	}

	public float floatValue() {
		return value;
	}
	
	public int hashCode() {
		int hash = super.hashCode();
		hash = 79 ^ hash ^ sortOrder;
		return hash;
	}

	public int intValue() {
		return value;
	}
	
	/**
	 * Not sure whether to make this public or not...
	 * would result in high-sort MissingValues, but descending
	 * sort may already take care of this.
	 */
	protected void invertValue() {
		value = value == Integer.MAX_VALUE ? Integer.MIN_VALUE : Integer.MAX_VALUE;
		sortOrder = sortOrder == 1 ? -1 : 1;
	}

	public long longValue() {
		return value;
	}
	
	public int sortOrder() {
		return sortOrder;
	}

	public String toString() {
		return rep;
	}
	
	public Number valueOf() {
		return value;
	}
}
