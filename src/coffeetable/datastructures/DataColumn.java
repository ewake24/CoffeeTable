package coffeetable.datastructures;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import coffeetable.interfaces.VectorUtilities;
import coffeetable.math.Infinite;
import coffeetable.math.MissingValue;
import coffeetable.math.TheoreticalValue;
import coffeetable.utils.MissingValueException;
import coffeetable.utils.SchemaMismatchException;


/**
 * A column object for DataTable. When used with DataTable, able to convert to various types, similar
 * to R vectors (WARNING: NO SUPPORT FOR CONVERTING TO LONG TYPE VARIABLES FROM OTHER TYPES).  Additionally,
 * implements many statistical methods for data analysis.
 * @author Taylor G Smith
 * @param <T>
 */
public class DataColumn<T extends Comparable<? super T>> extends ArrayList<T> implements java.io.Serializable, VectorUtilities<T> {
	private static final long serialVersionUID = 1872913776989418759L;
	private final static int defaultSize = 15;
	private String name;
	
	/* Declared transient/static with Object instead of T for serialization purposes */
	private transient final static Comparator<Object> comparator = new Comparator<Object>() {
		@Override
		public int compare(Object s1, Object s2) {
			return Integer.valueOf(s1.toString().length()).
					compareTo(Integer.valueOf(s2.toString().length()));
		}
	};
	
	/*Caching fields for checks to avoid repeated searches and calculations*/
	private boolean checkedForNumericism;
	private boolean isNumeric;
	private boolean checkedForConvertable;
	private boolean isConvertable;
	private boolean widthCalculated; //Cache, false for every update
	private int width;
	private Class<?> type;
	private Class<?> conversionType;
	
	private boolean checkedForNAs;
	private boolean containsNAs;
	
	public DataColumn(int initSize) {
		super(initSize);
		name = "DataColumn";
		widthCalculated = false;
		checkedForNumericism = false;
		checkedForConvertable = false;
		checkedForNAs = false;
		
		width = 0;
		containsNAs = false;
		isConvertable = false;
		isNumeric = false;
		type = null;
		conversionType = null;
	}
	
	public DataColumn() {
		this(defaultSize);
	}
	
	public DataColumn(Collection<T> arg0) {
		this(arg0.size());
		this.addAll(arg0);
	}
	
	public DataColumn(Collection<T> arg0, String name) {
		this(arg0.size());
		this.addAll(arg0);
		setName(name);
	}
	
	public DataColumn(String name) {
		this();
		setName(name);
	}
	
	/**
	 * Another DataTable constructor for when the number of rows is known.
	 * Used for reading in from CSV -- type is set to String
	 * @param trusted
	 * @param size
	 */
	protected DataColumn(boolean trusted, int size) {
		this(size);
		type = String.class;
	}
	
	/**
	 * Protected constructor for transformations from
	 * DataRow to DataColumn
	 * @param row
	 */
	@SuppressWarnings("unchecked")
	protected DataColumn(DataRow row) {
		//Don't need to check for singularity, because it was checked from DataRow
		for(Object o : row)
			this.addFromTrusted((T) o);
		this.isNumeric = row.schema().isNumeric();
		this.checkedForNumericism = true;
		this.type = row.schema().getContentClass();
		this.setName(row.name());
	}
	
	
	/* PRIVATE CLASSES FOR OPERATIONS */
	/**
	 * A private static class for use with numerically-typed DataColumns. Implements various
	 * statistical and vector-based operations in a fashion similar to an R dataframe.
	 * 
	 * Unless otherwise specified by an internal method, any column passed in will implicitly
	 * be converted to character for Double.parseDouble(...) to work.  This is due to the nature
	 * of generics not allowing Object casting.  Thus, you will see many String casting operations
	 * or String comparators as a workaround.
	 * @author Taylor G Smith
	 */
	@SuppressWarnings("rawtypes")
	private static class ArithmeticOperations {
		/**
		 * Comparator for sorting a column by double value
		 */
		private final static Comparator<String> compar = new Comparator<String>() {
			public int compare(String arg0, String arg1) {
				return Double.valueOf(Double.parseDouble(arg0)).compareTo(Double.valueOf(Double.parseDouble(arg1)));
			};
		};
		
		/**
		 * Comparator for sorting a column based on object frequency
		 */
		private final static Comparator<Map.Entry<Object,Integer>> freqCompar = new Comparator<Map.Entry<Object,Integer>>() {
			public int compare(Map.Entry<Object,Integer> arg0, Map.Entry<Object,Integer> arg1) {
				return arg0.getValue().compareTo(arg1.getValue());
			}
		};
		
		/**
		 * Ensure column can be worked with
		 * @param col
		 * @return true if the column is valid for arithmetic
		 */
		private static boolean check(DataColumn col) {
			return col.isNumeric() || col.isConvertableToNumeric();
		}
		
		/**
		 * Set of conditions a method with two cols must adhere to.
		 * @param col
		 * @param col2
		 */
		private static void dualColumnExceptionHandling(DataColumn col, DataColumn col2) {
			if(col.size() != col2.size())
				throw new IllegalArgumentException("Dim mismatch in Euclidean distance");
		}
		
		/**
		 * Set of conditions any method must adhere to.
		 * @param col
		 * @return an arithmetically-prepared DataColumn
		 */
		private static DataColumn exceptionHandling(DataColumn col) {
			exceptionHandlingNoConversion(col);
			/* This converts the temp column to a string column because	
			 * the double parsing requires a String and casting to String is faster than
			 * converting to numeric, back to string and back to numeric.*/
			if(!col.contentClass().equals(String.class))
				return col.asCharacter();
			else return col;
		}
		
		/**
		 * Check for column population
		 * @param col
		 */
		private static void exceptionHandlingNoConversion(DataColumn col) {
			if(col.isEmpty())
				throw new NullPointerException();
			if(!check(col))
				throw new NumberFormatException();
		}
		
		/**
		 * Calculate vector similarity between two columns
		 * @param col
		 * @param col2
		 * @return similarity score between two vectors
		 */
		public static double euclideanDistance(DataColumn col, DataColumn col2) {
			col = exceptionHandling(col);
			col2 = exceptionHandling(col2);
			dualColumnExceptionHandling(col,col2);
			
			double answer = 0;
			for(int i = 0; i < col.size(); i++) {
				answer += Math.pow(new Double((String)col.get(i)) - 
							new Double((String)col2.get(i)), 2);
			}
			return Math.sqrt(answer);
		}
		
		private static DataColumn filterMissing(DataColumn col) {
			for(int i = 0; i < col.size(); i++) {
				if(TheoreticalValue.isTheoretical((col.get(i))))
					col.remove(i--); //Subtract to move back and reassess new position
			}
			return col;
		}
		
		/**
		 * Calculate inner product between two columns (vectors)
		 * @param col
		 * @param col2
		 * @return inner product between two vectors
		 */
		public static double innerProduct(DataColumn col, DataColumn col2) {
			col = exceptionHandling(col);
			col2 = exceptionHandling(col2);
			dualColumnExceptionHandling(col,col2);
			
			double answer = 0;
			for(int i = 0; i < col.size(); i++) {
				answer += (new Double((String)col.get(i)) * 
						  new Double((String)col2.get(i)) );
			}
			return answer;
		}
		
		/**
		 * Identify maximum in column
		 * @param col
		 * @return max in column
		 */
		@SuppressWarnings("unchecked")
		public static double max(DataColumn col) {
			col = exceptionHandling(col);
			
			if(col.contains("NA") || col.contains("Infinity") || col.contains("-Infinity")) //MISSING VAL
				col = filterMissing(col);
			if(col.isEmpty())
				throw new MissingValueException("Cannot perform arithmetic operation on entirely theoretical row");
			
			Collections.sort(col, compar);
			return Double.valueOf((String)col.get(col.size()-1));
		}
		
		/**
		 * Calculate mean of column
		 * @param col
		 * @return mean of column
		 */
		public static double mean(DataColumn col) {
			return meanWithCheck(col, false);
		}
		
		/**
		 * Private internal method to save time if exceptions are already handled
		 * and column is already converted to character (see: sumWithCheck())
		 * @param col
		 * @param check
		 * @return
		 */
		private static double meanWithCheck(DataColumn col, boolean check) {
			col = filterMissing(col);
			return sumWithCheck(col, check) / col.size();
		}
		
		/**
		 * Identify minimum in a column
		 * @param col
		 * @return min in column
		 */
		@SuppressWarnings("unchecked")
		public static double min(DataColumn col) {
			col = exceptionHandling(col);
			
			if(col.contains("NA")) //MISSING VAL
				col = filterMissing(col);
			if(col.isEmpty())
				throw new MissingValueException("Cannot perform arithmetic operation on all-NA row");
				
			Collections.sort(col, compar);
			int comp = 0;
			if(TheoreticalValue.isTheoretical(col.get(0))) {
				while( !(TheoreticalValue.isTheoretical(col.get(++comp))) )
					continue;
			}
			return Double.valueOf((String)col.get(comp));
		}
		
		/**
		 * Identify the most-frequently occurring object in a column
		 * @param col
		 * @return most commonly occurring item in column
		 */
		@SuppressWarnings("unchecked")
		public static Object mode(DataColumn col) {
			exceptionHandlingNoConversion(col);
			
			col = filterMissing(col);
			if(col.isEmpty())
				throw new MissingValueException("Cannot perform arithmetic operation on all-NA row");
			
			/* Less for it to iterate over now */
			HashSet<Object> rawOcc = new HashSet<Object>();
			rawOcc.addAll(col);
			
			HashMap<Object,Integer> hm = new HashMap<Object,Integer>();
			ArrayList<Map.Entry<Object,Integer>> list = new ArrayList<Map.Entry<Object,Integer>>();
			for(Object o : rawOcc)
				hm.put(o, Collections.frequency(col,o));
			list.addAll(hm.entrySet());
			
			/* Sort by occurrence */
			Collections.sort(list, freqCompar);
			return list.get(list.size()-1).getKey();
		}
		
		public static double range(DataColumn col) {
			return max(col) - min(col);
		}
		
		@SuppressWarnings("unchecked")
		public static DataColumn scaleByFactor(DataColumn col, double scalar) {
			col = exceptionHandling(col);
			
			DataColumn newD = new DataColumn(col.size());
			newD.checkedForConvertable = true;
			newD.isNumeric = true;
			
			for(Object s : col) {
				if(MissingValue.isNA(s))
					newD.add(new MissingValue());
				else if(Infinite.isInfinite(s))
					newD.add(new Infinite(s.toString()));
				else newD.add( Double.valueOf((String)s)*scalar );
			}
			return newD;
		}
		
		/**
		 * Calculate the standard deviation of a column
		 * @param col
		 * @return standard deviation of column
		 */
		public static double standardDeviation(DataColumn col) {
			return Math.sqrt(variance(col));
		}
		
		/**
		 * Calculate the sum of all the elements in a column
		 * @param col
		 * @return sum of column
		 */
		public static double sum(DataColumn col) {
			return sumWithCheck(col, false);
		}
		
		public static void summary(DataColumn col) {
			double sum = sum(col);
			double mean = mean(col);
			double std = standardDeviation(col);
			double max = max(col);
			double min = min(col);
			System.out.println(col.name());
			System.out.println("Sum:\t\t\t" + sum);
			System.out.println("Mean:\t\t\t" + mean);
			System.out.println("Standard Deviation:\t" + std);
			System.out.println("Max:\t\t\t" + max);
			System.out.println("Min:\t\t\t" + min);
			System.out.println("Width:\t\t\t" + col.width());
			System.out.println("Type:\t\t\t" + col.contentClass());
			System.out.println("Size:\t\t\t" + col.size());
		}
		
		/**
		 * Many operations require calling sum first. We don't want to have to
		 * repeatedly call the exception handling if not necessary, so this can
		 * be called internally if we have already exception-handled / converted
		 * to character type
		 * @param col
		 * @param check
		 * @return sum of column
		 */
		private static double sumWithCheck(DataColumn col, boolean check) {
			if(!check)
				col = exceptionHandling(col);
			
			if(col.contains("NA")) //MISSING VAL
				col = filterMissing(col);
			if(col.isEmpty())
				throw new MissingValueException("Cannot perform arithmetic operation on all-NA row");
			
			double sum = 0;
			for(int i = 0; i < col.size(); i++) {
				sum += new Double((String)col.get(i));
			}
			return sum;
		}
		
		/**
		 * Calculate the variance of a column
		 * @param col
		 * @return variance of column
		 */
		public static double variance(DataColumn col) {
			col = exceptionHandling(col);
			
			if(col.contains("NA")) //MISSING VAL
				col = filterMissing(col);
			if(col.isEmpty())
				throw new MissingValueException("Cannot perform arithmetic operation on all-NA row");
			
			double avg = meanWithCheck(col,true);
			double sum = 0;
			for( Object o : col ) {
				sum += (new Double((String)o) - avg) * (new Double((String)o) - avg);
			}
			return sum / (col.size()-1);
		}
	}
	
	/**
	 * Implementation of Map.Entry for use in sorting a DataTable --
	 * will couple the Object with its index for index tracking in a sort
	 * @author Taylor G Smith
	 * @param <K>
	 * @param <V>
	 */
	private final class SortableEntry<K,V> implements Map.Entry<K,V> {
		private final K key;
		private V value;
		
		public SortableEntry(K key, V value) {
			this.key = key;
			this.value = value;
		}
		
		public K getKey() {
			return key;
		}
		
		public V getValue() {
			return value;
		}
		
		public V setValue(V value) {
			V old = this.value;
			this.value = value;
			return old;
		}
	}
	
	
	/**
	 * Skip type safety check -- only called form CsvParser
	 * @param element
	 * @return true if successfully added
	 */
	protected boolean addFromTrusted(T element) {
		return super.add(element);
	}
	
	/**
	 * Skip type safety check -- only called form CsvParser
	 * @param element
	 * @param index
	 */
	protected void addFromTrusted(T element, int index) {
		super.add(index, element);
	}
	
	/**
	 * Adds the given object to the DataColumn
	 * @return true if successfully added
	 */
	public boolean add(T element) {
		if(!this.isEmpty())
			if(!ensureTypeSafety(element))
				throw new SchemaMismatchException("Argument class does not match column class type");
		columnUpdate();
		return super.add( element );
	}
	
	/**
	 * Adds the given object to the DataColumn at the 
	 * specified index
	 */
	public void add(int index, T element) {
		if(!this.isEmpty())
			if(!ensureTypeSafety(element))
				throw new SchemaMismatchException("Argument class does not match column class type");
		columnUpdate(); //WAS JUST ELEMENT BEFORE
		super.add(index, element );
	}
	
	/**
	 * Adds the given collection of objects to the DataColumn
	 * @return true if successfully added
	 */
	public boolean addAll(Collection<? extends T> arg0) {
		if(!ensureTypeSafety(arg0))
			throw new SchemaMismatchException("Argument class does not match column class type");
		columnUpdate();
		return super.addAll(arg0);
	}
	
	/**
	 * Adds the given collection of objects to the DataColumn
	 * beginning at the specified index
	 */
	public boolean addAll(int index, Collection<? extends T> arg1) {
		if(!ensureTypeSafety(arg1))
			throw new SchemaMismatchException("Argument class does not match column class type");
		columnUpdate();
		return super.addAll(index, arg1);
	}
	
	/**
	 * Will return a version of the DataColumn with all contents
	 * converted to String.
	 * @return a string-converted instance of DataColumn
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public final DataColumn<String> asCharacter() {
		if(contentClass().equals(String.class))
			return (DataColumn) this;
		Collection<String> coll = new ArrayList<String>();
		for(T t : this) {
			coll.add(t.toString());
		}
		DataColumn<String> data = new DataColumn<String>(coll);
		data.isConvertable = isConvertable;
		data.checkedForConvertable = checkedForConvertable;
		data.isNumeric = false;
		data.checkedForNumericism = false;
		data.type = String.class;
		data.conversionType = conversionType;
		if(widthCalculated) {
			data.width = width;
			data.widthCalculated = true;
		}
		data.name = name;
		return data;
	}
	
	/**
	 * Protected helper methods for DataTable converting items
	 * to specific TYPES of numeric -- Double
	 * @return a double-converted DataColumn
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private final DataColumn<Double> asDouble() {
		if(contentClass().equals(Double.class))
			return (DataColumn) this;
		ArrayList collection = new ArrayList(this.size());
		for(T t : this) {
			String tar = contentClass().equals(String.class) ? (String) t : t.toString();
			if(MissingValue.isNA(tar))
				collection.add(new MissingValue());
			else if(Infinite.isInfinite(tar))
				collection.add(new Infinite(tar));
			else collection.add( new Double(tar) );
		}
		DataColumn<Double> returnable = new DataColumn<Double>(collection);
		returnable.type = Double.class;
		returnable = (DataColumn) asNumericUtilities(returnable, this);
		return returnable;
	}
	
	/**
	 * Protected helper methods for DataTable converting items
	 * to specific TYPES of numeric -- Integer
	 * @return an integer-converted DataColumn
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private final DataColumn<Integer> asInteger() {
		if(contentClass().equals(Integer.class))
			return (DataColumn) this;
		ArrayList collection = new ArrayList(this.size());
		for(T t : this) {
			String tar = contentClass().equals(String.class) ? (String) t : t.toString();
			if(MissingValue.isNA(tar))
				collection.add(new MissingValue());
			else if(Infinite.isInfinite(tar))
				collection.add(new Infinite(tar));
			else collection.add( new Integer(tar) );
		}
		DataColumn<Integer> returnable = new DataColumn<Integer>(collection);
		returnable.type = Integer.class;
		returnable = (DataColumn) asNumericUtilities(returnable, this);
		return returnable;
	}
	
	/**
	 * Will return a clone of the current DataColumn parsed to the most
	 * appropriate determined numeric type
	 * @return a numerically-converted DataColumn
	 */
	@SuppressWarnings({ "unchecked" })
	public final DataColumn<? extends Number> asNumeric() {
		if(isNumeric())
			return (DataColumn<? extends Number>) this;
		else if(isConvertableToNumeric()) {
			Class<? extends Object> converter = numericConversionType();
			if(converter.equals(Double.class)) {
				return asDouble();
			} else {
				return asInteger();
			}
		} else throw new NumberFormatException("Cannot parse column to numeric");
	}
	
	/**
	 * Utilities for when a column is converted to some type of numeric. These are 
	 * all attributes the new datacolumn will possess post-conversion
	 * @param returnable
	 * @param original
	 * @return the numerically-converted DataColumn with cloned attributes
	 */
	private final static DataColumn<?> asNumericUtilities(DataColumn<?> returnable, DataColumn<?> original) {
		returnable.isNumeric = true;
		returnable.checkedForNumericism = true;
		returnable.isConvertable = true;
		returnable.checkedForConvertable = true;
		returnable.checkedForNAs = original.checkedForNAs;
		returnable.containsNAs = original.containsNAs;
		if(original.widthCalculated) {
			returnable.width = original.width;
			returnable.widthCalculated = true;
		}
		returnable.conversionType = returnable.type;
		returnable.name = original.name;
		return returnable;
	}
	
	/**
	 * Will reset this instance to default but will retain the column name
	 */
	public void clear() {
		columnUpdate(); //Takes care of a few
		isConvertable = false;
		isNumeric = false;
		type = null;
		conversionType = null;
		containsNAs = false;
		width = 0;
		super.clear();
	}
	
	/**
	 * Creates a copy of this instance minus transient attributes
	 */
	@SuppressWarnings("unchecked")
	public Object clone() {
		DataColumn<T> clone = null;
		try {
			clone = (DataColumn<T>) super.clone();
		} catch(Exception e) {
			throw new InternalError();
		}
		
		if(widthCalculated) {
			clone.widthCalculated = true;
			clone.width = width;
		} else {
			clone.widthCalculated = false;
			clone.width = 0;
		}
		
		if(checkedForNumericism) {
			clone.checkedForNumericism = true;
			clone.isNumeric = isNumeric;
		} else {
			clone.checkedForNumericism = false;
			clone.isNumeric = false;
		}
		
		if(checkedForConvertable) {
			clone.checkedForConvertable = true;
			clone.isConvertable = isConvertable;
			clone.conversionType = conversionType;
		} else {
			clone.checkedForConvertable = false;
			clone.isConvertable = false;
			clone.conversionType = null;
		}
		clone.type = type;
		clone.checkedForNAs = checkedForNAs;
		clone.containsNAs = containsNAs;
		return clone;
	}
	
	/**
	 * To be called any time the column is updated;
	 * resets the widthCalculated cache parameter as well
	 * as other parameters critical in exception handling.
	 * 
	 * Can also be called if a DataTable performs a subsetting
	 * operation
	 */
	protected final void columnUpdate() {
		widthCalculated = false;
		/* These need to be reset in case a dumb user
		 * is using an unchecked addition to the column.
		 * Must enforce numericism NO MATTER WHAT */
		checkedForNumericism = false;
		checkedForConvertable = false;
		checkedForNAs = false;
		containsNAs = false;
	}
	
	/**
	 * Will return whether the column contains
	 * ANY theoretical values (NA, Infinite or NaN)
	 */
	public boolean containsNA() {
		if(containsNAs)
			return true;
		if(this.isEmpty())
			return false;
		else if(checkedForNAs && !containsNAs)
			return false;
		
		checkedForNAs = true;
		for(T t : this) {
			if(TheoreticalValue.isTheoretical(t))
				return ( containsNAs = true);
		}
		return false;
	}
	
	/**
	 * Will return the Class of the objects held within the column
	 * @return the class of the column contents
	 */
	public Class<?> contentClass() {
		if(!(null==type))
			return type;
		else if(this.isEmpty())
			return null;
		else {
			for(T t : this) {
				if(!TheoreticalValue.isTheoretical(t))
					return (type = t.getClass()); //Legal?
				else continue;
			}
		}
		return type;
	}
	
	/**
	 * Will count the number of theoretical values (NA, Infinite, NaN)
	 * within the column
	 */
	public int countMissingValues() {
		if(this.isEmpty())
			return 0;
		else if(!containsNA())
			return 0;
		int sum = 0;
		for(T t : this) {
			if(TheoreticalValue.isTheoretical(t))
				sum += 1;
		}
		return sum;
	}
	
	/**
	 * For those 'unchecked addition' users out there... this method
	 * helps keep you in line! Ensures no one tries to add elements of different types
	 * @param element
	 * @return whether classes are typesafe
	 */
	private final boolean ensureTypeSafety(Object element) {
		/* Shouldn't be empty if this is called due to checking
		 * at other stages, but this is just in case..... */
		if(this.isEmpty()) {
			type = element.getClass();
			return true;
		} else if( TheoreticalValue.isTheoretical(element) ) {
			containsNAs = true;
			return true;
		} else if( null==contentClass() || TheoreticalValue.class.isAssignableFrom(contentClass()) ) {
			type = element.getClass();
			return true;
		} else return element.getClass().equals(this.contentClass());
	}
	
	/**
	 * Same as the ensureTypeSafety method but for batch additions
	 * @param element
	 * @return whether classes are typesafe
	 */
	private final boolean ensureTypeSafety(Collection<? extends T> element) {
		if(!this.isEmpty()) {
			for(Object e : element) {
				if(!ensureTypeSafety(e))
					return false;
			}
			return true;
		} else {
			HashSet<Class<?>> classes = new HashSet<Class<?>>();
			Class<?> tmp = null;
			for(Object e : element) {
				if(TheoreticalValue.isTheoretical(e)) {
					containsNAs = true;
					continue;
				}
				tmp = e.getClass();
				classes.add(tmp);
				if(classes.size() > 1) return false;
			}
			type = tmp;
			return true;
		}
	}
	
	/**
	 * Returns the vector similarity (or Euclidean distance) between
	 * this instance of the DataColumn and another. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @param arg0
	 * @return vector similarity
	 */
	@SuppressWarnings("rawtypes")
	public double euclideanDistance(DataColumn arg0) {
		return ArithmeticOperations.euclideanDistance(this, arg0);
	}
	
	/**
	 * Returns the inner product between this instance of the DataColumn and
	 * another. NOTE: this will throw an exception for a datacolumn that is 
	 * either NOT NUMERIC or NOT CONVERTABLE to a numeric type
	 * @param arg0
	 * @return inner product
	 */
	@SuppressWarnings("rawtypes")
	public double innerProduct(DataColumn arg0) {
		return ArithmeticOperations.innerProduct(this, arg0);
	}
	
	/**
	 * Returns whether the DataColumn could be parsed to a numeric type 
	 * (either Integer or Double)
	 * @return whether the DataColumn can be converted to a numeric type
	 */
	public boolean isConvertableToNumeric() {
		if(isNumeric()) {
			checkedForConvertable = true;
			isConvertable = true;
			conversionType = contentClass();
			return true;
		} else if(checkedForConvertable)
			return isConvertable;
		if( numericConversionType()==null ) {
			checkedForConvertable = true;
			isConvertable = false;
			return false;
		}
		checkedForConvertable = true;
		isConvertable = true;
		return true;
	}
	
	/**
	 * Returns whether the DataColumn is currently a 
	 * numeric type (assignable from Number.class)
	 * @return whether the column is numeric
	 */
	public boolean isNumeric() {
		if(!checkedForNumericism) {
			if(this.isEmpty())
				return false;
			isNumeric = Number.class.isAssignableFrom(contentClass());
			checkedForNumericism = true;
			return isNumeric;
		} else
			return isNumeric;
	}
	
	/**
	 * Returns the maximum number in the DataColumn. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @return max of column
	 */
	public double max() {
		return ArithmeticOperations.max(this);
	}
	
	/**
	 * Returns the mean of the DataColumn. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @return mean of column
	 */
	public double mean() {
		return ArithmeticOperations.mean(this);
	}
	
	/**
	 * Returns the minimum number in the DataColumn. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @return min in column
	 */
	public double min() {
		return ArithmeticOperations.min(this);
	}
	
	/**
	 * Returns the most frequently occurring object in the DataColumn. 
	 * NOTE: this will work for ANY object type, not just numeric
	 * @return mode of column
	 */
	@SuppressWarnings("unchecked")
	public T mode() {
		return (T) ArithmeticOperations.mode(this);
	}
	
	/**
	 * Returns the name of the DataColumn ('DataColumn' if not set)
	 */
	public final String name() {
		return name;
	}
	
	/**
	 * If is numeric or can be converted to numeric, will
	 * return the best numeric class the column is capable of
	 * coverting to.  Searches either the first half of the DataColumn
	 * (or the entire thing if its size is less than 20) to determine
	 * to which numeric class the column could best be converted.
	 * 
	 * NOTE: If your column is not returning the conversion type you
	 * expected, please email the author and the cutoff will be incremented
	 * higher -- this is a beta feature right now.
	 * @return the class the column will convert to if called 'asNumeric()'
	 */
	protected Class<?> numericConversionType() {
		if(isNumeric())						//The generic type was declared so we know it
			return type;
		else if(!(null == conversionType))	//We have already found the type
			return conversionType;
		else {								//It is a string type -- need to find conversion
			int cutoff = this.size() < 20 ? this.size() : //If it's too short, just loop it all
							this.size()/2;	//For now let's cutoff at the halfway point to save time
			int start = 0;
			String col = super.toString().toLowerCase();
			boolean possibleDouble = col.contains("e") || col.contains(".");
			for(T t : this) {
				if(start++ == cutoff)		//Now we only look at half the column to determine
					return conversionType;
				String ts = t.toString(); 	//String version for pattern matching...
				if( numberCouldBeInteger(ts) ) {
					conversionType = Integer.class;
				} else if( possibleDouble && numberCouldBeDouble(ts) ) {
					return (conversionType = Double.class); 	//Hierarchical. If double, automatically return double
				} else if(!TheoreticalValue.isTheoretical(t)) return null; 	//Skip out early if it isnt int, double, NA, inf, etc
			}
			return conversionType;
		}
	}
	
	private final boolean numberCouldBeInteger(String ts) {
		return ts.matches("-?[0-9]+"); // << Should handle everything
	}
	
	private final boolean numberCouldBeDouble(String ts) {
		return ts.matches("[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?");
				//ts.matches("-?\\d|\\.+(\\d+)?+[eE][-+]?\\d");
	}
	
	/**
	 * Prints the DataColumn (duh)
	 */
	public void print() {
		if( !isEmpty() ) {
			if ( !(name==null) )
				System.out.println( name );
			for( T t : this )
				System.out.println( t.toString() );
			System.out.println();
		} else return;
	}

	/**
	 * Removes the object from the DataColumn
	 */
	public boolean remove(Object arg0) {
		if(this.size()==1) {
			clear();
			return true;
		}
		columnUpdate();
		return super.remove(arg0);
	}
	
	/**
	 * Removes the object at the corresponding index
	 */
	public T remove(int index) {
		if(this.size()==1) {
			T holdover = super.remove(index);
			clear();
			return holdover;
		}
		columnUpdate();
		return super.remove(index);
	}
	
	/**
	 * Removes all objects within the parameter 
	 * collection from the DataColumn
	 */
	public boolean removeAll(Collection<?> arg0) {
		if(this.size()==1) {
			clear();
			return true;
		}
		columnUpdate();
		return super.removeAll(arg0);
	}
	
	/**
	 * Renders the DataColumn for printing within a DataTable.
	 * This is to identify the width of the column for spacing
	 */
	private final void render() {
		width = sortByLength();
		widthCalculated = true;
	}
	
	/**
	 * Checks for TheoreticalValues in a comparator operation and
	 * returns the appropriate value for sorting
	 * @return
	 */
	private final int returnInComparator(T s1, T s2) {
		boolean s1NA = MissingValue.isNA(s1);
		boolean s2NA = MissingValue.isNA(s2);
		boolean s1Inf = Infinite.isInfinite(s1);
		boolean s2Inf = Infinite.isInfinite(s2);
		
		if(s1NA || s2NA || s1Inf || s2Inf) {
			if(s1NA ^ s2NA)
				return s1NA ? -1 : 1;
			if(s1NA && s2NA)
				return 0;
			
			if(s1Inf ^ s2Inf) {	//Only one inf
				if(s1Inf)		//The first is inf
					return Infinite.sortOrder((Infinite)s1);	//What's its sort order?
				
				/* The first is not infinite, but the second is. Return the first's
				 * position in relation to the second's sort order (-1 * val) */
				return Infinite.sortOrder((Infinite)s2) * -1;
			} else if(s1Inf && s2Inf) return ((Infinite)s1).compareTo((Infinite)s2);
				/*return Integer.valueOf(Infinite.sortOrder((Infinite)s1))
						.compareTo( Integer.valueOf(Infinite.sortOrder((Infinite)s2)) );*/
		}
		return s1.compareTo(s2);
	}
	
	/**
	 * Sets the corresponding index to the passed-in Object
	 */
	public T set(int index, T element) {
		if(this.size() > 0)
			ensureTypeSafety(element);
		columnUpdate();
		return super.set(index, element);
	}
	
	/**
	 * Assigns the column a name
	 */
	public void setName(String name) {
		this.name = (null == name || name.isEmpty()) ? "DataColumn" : 
						(name.equals("DataRow") ? "DataColumn" : name);
		if(widthCalculated && this.name.length() > width)
			width = this.name.length();
	}
	
	/* -- Intra-DataTable sorts -- */
	/**
	 * Sorts the actual map entries
	 * @return a comparator to sort a collection of map entries
	 */
	private final Comparator<Map.Entry<T,Integer>> mapEntrySorter() {
		return new Comparator<Map.Entry<T,Integer>>() {
			@Override
			public int compare(Map.Entry<T,Integer> s1, Map.Entry<T,Integer> s2) {
				return returnInComparator(s1.getKey(),s2.getKey());
			}
		};
	}
	
	public double range() {
		return ArithmeticOperations.range(this);
	}
	
	@SuppressWarnings("rawtypes")
	public final static DataColumn readFromSerializedObject(FileInputStream fileIn) throws IOException, ClassNotFoundException {
		DataColumn d = null;
		ObjectInputStream in = new ObjectInputStream(fileIn);
		d = (DataColumn) in.readObject();
		in.close();
		fileIn.close();
		return d;
	}
	
	@SuppressWarnings("unchecked")
	public DataColumn<Double> scaleByFactor(double scalar) {
		return ArithmeticOperations.scaleByFactor(this,scalar);
	}

	/**
	 * Generate item : index list of map entries
	 * @return a list of map entries (Object : index in column)
	 */
	private final List<Map.Entry<T,Integer>> sortableMapEntries() {
		int index = 0;
		List<Map.Entry<T,Integer>> returnable = new ArrayList<Map.Entry<T,Integer>>();
		for(T t : this)
			returnable.add(new SortableEntry<T,Integer>(t, index++));
		return returnable;
	}
	
	/**
	 * Extract the values from the sorted map entries
	 * @param sorted
	 * @return the indices of sorted objects
	 */
	private final List<Integer> sortedMapEntryValues(List<Map.Entry<T,Integer>> sorted) {
		List<Integer> arr = new ArrayList<Integer>();
		for(Map.Entry<T,Integer> o : sorted)
			arr.add(o.getValue());
		return arr;
	}
	
	protected List<Integer> sortedAscendingMapEntries() {
		List<Map.Entry<T,Integer>> sortables = sortableMapEntries();
		Collections.sort(sortables, mapEntrySorter());
		return sortedMapEntryValues(sortables);
	}
	
	protected List<Integer> sortedDescendingMapEntries() {
		List<Map.Entry<T,Integer>> sortables = sortableMapEntries();
		Collections.sort(sortables, mapEntrySorter());
		Collections.reverse(sortables);
		return sortedMapEntryValues(sortables);
	}

	
	/* -- PLAIN SORTS -- */
	/**
	 * To be used with render() -- this sorts the column by string length
	 * @return the highest length item in the column
	 */
	private final int sortByLength() {
		ArrayList<T> l = new ArrayList<T>(this);
		Collections.sort(l, comparator);
		int max = l.get(l.size() - 1).toString().length();
		return name.length() > max ? name.length() : max;
	}
	
	/**
	 * The comparator to be used for default col sorts (not in a DataTable)
	 * NAs or MissingValue objects will sort LOW for ascending, 
	 * HIGH otherwise.
	 * @return a comparator to sort the column by object length
	 */
	private final Comparator<T> sortableComparator() {
		return new Comparator<T>() {
			@Override
			public int compare(T s1, T s2) {
				return returnInComparator(s1,s2);
			}
		};
	}
	/**
	 * Sorts the DataColumn ascending (NAs low)
	 */
	protected void sortAscending() {
		Collections.sort(this, sortableComparator());
	}
	
	/**
	 * Sorts the DataColumn descending (NAs high)
	 */
	protected void sortDescending() {
		Collections.sort(this, Collections.reverseOrder(sortableComparator()));
	}
	
	/**
	 * Calculates the standard deviation of the column. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @return standard deviation of column
	 */
	public double standardDeviation() {
		return ArithmeticOperations.standardDeviation(this);
	}
	
	public DataColumn<T> subsetByCondition(SubsettableCondition sub) {
		boolean[] keeps = subsetLogicalVector(sub);
		
		DataColumn<T> dc = new DataColumn<T>(new ArrayList<T>(this), this.name+"_Subset");
		int j = 0;
		for(int i = 0; i < this.size(); i++) {
			if(!keeps[i])
				dc.remove(j);
			else j++; //Only increments if true
		}
		dc.columnUpdate();
		return dc;
	}
	
	public boolean[] subsetLogicalVector(SubsettableCondition sub) {
		return sub.evaluate(this);
	}
	
	/**
	 * An arithmetic operation for use on a numeric-typed column.  
	 * NOTE: this will throw an exception for a datacolumn that is 
	 * either NOT NUMERIC or NOT CONVERTABLE to a numeric type
	 * @return sum of column
	 */
	public double sum() {
		return ArithmeticOperations.sum(this);
	}
	
	/**
	 * Will print the column's descriptive statistics, if any,
	 * else if will print info regarding the column's attributes
	 */
	public void summary() {
		try {
			ArithmeticOperations.summary(this);
		} catch(NumberFormatException e) { //If not numeric
			System.out.println(this.name);
			System.out.println("Width:\t" + this.width());
			System.out.println("Type:\t" + this.contentClass());
			System.out.println("Size:\t" + this.size());
		}
		System.out.println(System.getProperty("line.separator"));
	}
	
	/**
	 * Converts the column to an instance of DataRow
	 * @return the column parsed to DataRow
	 */
	public DataRow toDataRow() {
		DataRow d = new DataRow(this,name);
		return d;
	}
	
	/**
	 * Presents a String representation of the DataColumn
	 */
	public String toString() {
		return name + ": " + super.toString();
	}
	
	public Set<T> unique() {
		return new HashSet<T>(this);
	}
	
	/**
	 * Calculates the variance of the column. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @return variance of column
	 */
	public double variance() {
		return ArithmeticOperations.variance(this);
	}
	
	/**
	 * The width of the longest element in the DataColumn (for spacing purposes)
	 * @return widest element in column (for print rendering)
	 */
	public int width() {
		if(!widthCalculated)
			render();
		return width;
	}
	
	/**
	 * Writes a serialized DataColumn object from this instance,
	 * returns true if successful
	 * @throws IOException
	 * @param path - the path to which to write
	 * @return true if the operation was successful
	 */
	public final boolean writeObject(String path) throws IOException {
		if(null == path || path.isEmpty()) {
			path = "/tmp/datacolumn.ser"; 
			System.out.println("Path was empty, saving to "+path);
		} else if(!path.endsWith(".ser"))
			path += ".ser";
			
		FileOutputStream fileOut = new FileOutputStream(path);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(this);
		out.close();
		fileOut.close();
		return new File(path).exists();
	}
}
