package coffeetable.datastructures;

import java.util.Comparator;

/**
 * A class used to define a condition by which a DataTable's rows will
 * be subset. NOTE: a condition can not be predicated on the MissingValue
 * class, as this is equivalent to pointing to a null value. If you desire
 * to remove all NAs from a column, define the evaluator with the .removeNA()
 * method in the Builder class.
 * @author Taylor G Smith
 */
@SuppressWarnings("rawtypes")
public class SubsettableCondition {
	private final Object value;
	private boolean equals = false;
	private boolean lessThan = false;
	private boolean negate = false; //Should it perform the opposite?
	private final boolean removeNA;
	private Comparator<Comparable> comparator;
	
	public static enum Evaluator {
		EQUALS, LESSTHAN, GREATERTHAN
	}
	
	public static class Builder {
		private Object value;
		private Evaluator evaluator = null;
		private boolean removeNA = false;
		private boolean negate;
		
		public Builder(Evaluator evaluator, Object value) {
			this.evaluator = evaluator;
			this.value = value;
		}
		
		public Builder negate() {
			negate = true;
			return this;
		}
		
		public Builder removeNA() {
			removeNA = true;
			return this;
		}
		
		public SubsettableCondition build() {
			return new SubsettableCondition(this);
		}
	}
	
	public SubsettableCondition(Builder builder) {
		this.value = builder.value;
		this.equals = builder.evaluator.equals(Evaluator.EQUALS);
		this.lessThan = builder.evaluator.equals(Evaluator.LESSTHAN);
		this.negate = builder.negate;
		//Else it's greater than...
		
		this.removeNA = builder.removeNA;
		
		/*
		int trueCount = (equals ? 1 : 0)
				+ (lessThan ? 1 : 0)
				+ (greaterThan ? 1 : 0);
		
		if(trueCount != 1) //Check for people who assign more than one eval
			throw new IllegalArgumentException("Condition must have exactly ONE " +
												"evaluator (either equals, greaterthan " +
												"OR lessthan)");
		 */
		
		this.comparator = setComparator();
	}
	
	/**
	 * If the value is NA and removeNA is true, it will return 2 
	 * so as to always evaluate to false, else it will evaluate
	 * true so as to remain in the collection.
	 * @return the comparison value between two Comparables
	 */
	private Comparator<Comparable> setComparator() {
		return new Comparator<Comparable>() {
			@SuppressWarnings("unchecked")
			public int compare(Comparable s1, Comparable s2) {
				if( MissingValue.isNA(s1) && removeNA )
					return 2; //Always will evaluate to false
				else if( MissingValue.isNA(s1) ) //Implicit: && !removeNA
					return equals ? 0 : lessThan ? -1 : 1;
					
				return s1.compareTo(s2);
			};
		};
	}
	
	/**
	 * Will evaluate which items in the DataColumn meet the evaluation criteria
	 * @param col
	 * @return a boolean vector corresponding to values that meet the evaluation criteria
	 */
	public boolean[] evaluate(DataColumn col) {
		boolean[] bool = new boolean[col.size()];
		int comp = equals ? 0 :
					lessThan ? -1 : 
						1;	//Greater
		
		int index = 0;
		for(Object o : col) {
			int compare = comparator.compare((Comparable) o, (Comparable) value);
			if(removeNA && compare==2 && negate)
				bool[index++] = false;
			else
				bool[index++] = !negate ? compare == comp : compare != comp; //Non-negate:Negate
		}
		return bool;
	}
}
