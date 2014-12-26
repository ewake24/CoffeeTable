package coffeetable.datatypes;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashSet;

/**
 * A class for DataTable reminiscent of enumerated values, or categorical
 * variables found within a DataColumn/Row/Table. Can be used as factor levels
 * in predictive analytics packages
 * @author Taylor G Smith
 */
public final class Factor implements Comparable<Object>, Serializable {
	private static final long serialVersionUID = 910691899601707948L;
	private final String representation;
	private final int level;
	
	public Factor(String s, int level) {
		this.representation = s;
		this.level = level;
	}
	
	public boolean equals(Object o) {
		return Factor.isFactor(o) && o.hashCode() == this.hashCode();
	}
	
	/**
	 * Assign each String in a LinkedHashSet a factor level in order of appearance
	 * @param set
	 * @return Map of String : Integer
	 */
	public static HashMap<String,Integer> factorLevels(LinkedHashSet<String> set) {
		HashMap<String,Integer> map = new HashMap<String,Integer>();
		int i = 0;
		for(String s : set)
			map.put(s, i++);
		return map;
	}
	
	/**
	 * For a set of strings, will return a Map with the string representation
	 * to the factor representation of the String.
	 * @param set
	 * @return Map of String : Factor
	 */
	public static HashMap<String, Factor> factorMap(LinkedHashSet<String> set) {
		HashMap<String,Factor> map = new HashMap<String,Factor>();
		int i = 0;
		for(String s : set)
			map.put(s, new Factor(s,i++));
		return map;
	}
	
	public int hashCode() {
		return representation.hashCode() ^ level;
	}
	
	public static boolean isFactor(Object o) {
		return o instanceof Factor || o.getClass().equals(Factor.class);
	}
	
	/**
	 * The integer level of the factor
	 * @return the level of the factor
	 */
	public int level() {
		return level;
	}
	
	public String toString() {
		return representation;
	}

	public int compareTo(Object arg0) {
		return representation.compareTo(arg0.toString());
	}
}