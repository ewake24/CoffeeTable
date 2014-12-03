package coffeetable.datastructures;

import java.util.Comparator;

@SuppressWarnings("rawtypes")
public class SubsettableCondition {
	private final Object value;
	private boolean equals = false;
	private boolean lessThan = false;
	private final boolean removeNA;
	private Comparator<Comparable> comparator;
	
	public static enum Evaluator {
		EQUALS, LESSTHAN, GREATERTHAN
	}
	
	public static class Builder {
		private Object value;
		private Evaluator evaluator = null;
		private boolean removeNA = false;
		
		public Builder(Evaluator evaluator, Object value) {
			this.evaluator = evaluator;
			this.value = value;
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
	
	private Comparator<Comparable> setComparator() {
		return new Comparator<Comparable>() {
			@SuppressWarnings("unchecked")
			public int compare(Comparable s1, Comparable s2) {
				if( MissingValue.isNA(s1) && removeNA )
					return 2; //Always will evaluate to false
				
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
		for(Object o : col)
			bool[index++] = comparator.compare((Comparable) o, (Comparable) value) == comp;
		return bool;
	}
}
