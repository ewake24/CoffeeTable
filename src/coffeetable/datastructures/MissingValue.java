package coffeetable.datastructures;

/**
 * To be used in conjunction with DataTable in place of a <tt>null</tt> value.
 * MissingValue is a comparable class (both in its use as a null value as well as its
 * interface implementation) that can be sorted, treated as either numeric or 
 * String type and otherwise ignored.  Its Number value is that of <tt>Integer.MIN_VALUE</tt>
 * and will always sort low, unless a Comparator reverses the order.
 * @author Taylor G Smith
 * @see Number
 */
public class MissingValue extends Number implements Comparable<Number> {
	private static final long serialVersionUID = 8685839276702330957L;
	private static Integer value = Integer.MIN_VALUE;
	private static int sortOrder = -1;
	
	public MissingValue() {
		/* Init methods if any */
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
	
	protected static boolean isNA(Object o) {
		return (null == o)
				|| o.getClass().equals(MissingValue.class) 
				|| o.toString().equals("NA") 
				|| o.toString().equals("");
	}

	public float floatValue() {
		return value;
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

	public String toString() {
		return "NA";
	}
	
	public Number valueOf() {
		return value;
	}
}
