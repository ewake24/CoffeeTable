package coffeetable.datastructures;

import java.util.Comparator;

import coffeetable.math.TheoreticalValue;

/**
 * A class used to define a condition by which a DataTable's rows will
 * be subset. NOTE: to subset on the presence of a TheoreticalValue, the
 * only valid condition is EQUALS, as a value greater than, less than or otherwise
 * is not defined in relation to Infinite, MissingValue or NaN
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
	private boolean valueIsTheo;
	
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
		
		/**
		 * Will remove all TheoreticalValues if selected
		 * @return an instance of Builder
		 */
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
		this.comparator = setComparator();
		
		if((valueIsTheo = TheoreticalValue.isTheoretical(value)) && removeNA)
			throw new IllegalArgumentException("Cannot subset for TheoreticalValues and concurrently remove them");
		if(valueIsTheo && !equals)
			throw new IllegalArgumentException("Cannot evaluate for TheoreticalValue in any manner other than \"EQUALS\"");
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
			if(TheoreticalValue.isTheoretical(o)) {
				boolean pass = false;
				if(!removeNA)
					pass = true;
				if(negate)
					pass = valueIsTheo ? false : pass;
				bool[index++] = pass;
				continue;
			} else if(valueIsTheo) { //The current ob is not theo, but we only want theos (unless negate)
				bool[index++] = negate? true : false;
				continue;
			}
			
			int compare = comparator.compare((Comparable) o, (Comparable) value);
			bool[index++] = !negate ? compare == comp : compare != comp; //Non-negate:Negate
		}
		return bool;
	}
}
