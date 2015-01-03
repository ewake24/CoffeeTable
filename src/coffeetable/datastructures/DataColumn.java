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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import coffeetable.datatypes.Factor;
import coffeetable.math.Infinite;
import coffeetable.math.MissingValue;
import coffeetable.math.TheoreticalValue;
import coffeetable.utils.DimensionMismatchException;
import coffeetable.utils.MissingValueException;
import coffeetable.utils.SchemaMismatchException;


/**
 * A column object for DataTable. When used with DataTable, able to convert to various types, similar
 * to R vectors (WARNING: NO SUPPORT FOR CONVERTING TO LONG TYPE VARIABLES FROM OTHER TYPES).  Additionally,
 * implements many statistical methods for data analysis.
 * 
 * The DataColumn is the 'heavy lifter' of the DataTable family; it handles all typing, conversions, math,
 * etc. As such, many methods are declared final for the sake of maintaining homeostatic extensibility.
 * @author Taylor G Smith
 * @param <T>
 */
@SuppressWarnings("rawtypes")
public class DataColumn<T extends Comparable<? super T> & java.io.Serializable> extends Vector<T> implements java.io.Serializable {
	private static final long serialVersionUID = 1872913776989418759L;
	private final static int defaultSize = 15;
	
	/* Declared transient/static with Object instead of T for serialization purposes */
	private transient final static Comparator<Object> comparator = new Comparator<Object>() {
		@Override
		public int compare(Object s1, Object s2) {
			return Integer.valueOf(s1.toString().length()).
					compareTo(Integer.valueOf(s2.toString().length()));
		}
	};
	
	/**
	 * Comparator for sorting a column based on object frequency
	 */
	private transient final static Comparator<Map.Entry<Object,Integer>> freqCompar = new Comparator<Map.Entry<Object,Integer>>() {
		public int compare(Map.Entry<Object,Integer> arg0, Map.Entry<Object,Integer> arg1) {
			return arg0.getValue().compareTo(arg1.getValue());
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
		super(row.size());
		
		//Don't need to check for singularity, because it was checked from DataRow
		for(Object o : row)
			this.addFromTrusted((T) o);
		this.isNumeric = row.schema().isNumeric();
		this.checkedForNumericism = true;
		this.type = row.schema().getContentClass();
		this.setName(row.name());
	}
	
	
	
	/*--------------------------------------------------------------------*/
	/* PRIVATE CLASSES FOR OPERATIONS (ARITHMETIC, ETC) */
	/**
	 * Current as of version 1.5 -- inner class to handle all arithmetic operations
	 * @author Taylor G Smith
	 * @param <E>
	 */
	@SuppressWarnings("unchecked")
	final class NumericVector<E extends Number & Comparable<? super E>> extends DataColumn<E> {
		private static final long serialVersionUID = 2552736984902135876L;
		public final Class<?> content;
		private DataColumn<String> charRep;
		
		/**
		 * Comparator for sorting a column by double value
		 */
		private Comparator<String> compar = new Comparator<String>() {
			public int compare(String arg0, String arg1) {
				return Double.valueOf(Double.parseDouble(arg0)).compareTo(Double.valueOf(Double.parseDouble(arg1)));
			};
		};
		
		/* -- Helper methods -- */
		NumericVector(DataColumn<E> col) {
			super(col);
			if(!col.isNumeric())
				throw new NumberFormatException();
			if(col.isEmpty())
				throw new NullPointerException();
			
			setName(col.name);
			content = super.contentClass();
			charRep = super.asCharacter();
		}
		
		public Class<?> highestCommonConvertableClass(NumericVector col) {
			if(content.equals(Double.class) || col.content.equals(Double.class))
				return Double.class;
			else return Integer.class;
		}
		
		public E convertDoubleToInteger(double d) {
			return (E)new Integer((int) d);
		}
		
		public E convertDoubleToE(double d) {
			return (E)new Double(d);
		}
		
		public E convertIntegerToDouble(int i) {
			return (E)new Double((double) i);
		}
		
		public E convertIntegerToE(int i) {
			return (E)new Integer(i);
		}
		
		
		/* --- Math methods --- */
		public NumericVector<Double> center() {
			DataColumn dc = new DataColumn();
			double mean = mean();
			for(String e : this.charRep) {
				if(TheoreticalValue.isTheoretical(e))
					dc.add(Infinite.isInfinite(e) ? new Infinite() : new MissingValue());
				else dc.add(new Double(e) - mean);
			} return new NumericVector<Double>(dc);
		}
		
		public double distance(NumericVector<E> col2, int q) {
			if(this.size() != col2.size())
				throw new DimensionMismatchException("Dim mismatch");
			else if(q < 1)
				throw new IllegalArgumentException("q must be greater than zero");
			
			double answer = 0;
			for(int i = 0; i < this.size(); i++) {
				answer += Math.pow( Math.abs(new Double(charRep.get(i)) - new Double(col2.charRep.get(i))), q );
			}
			
			if(answer != 0)
				return Math.pow(Math.E, Math.log(answer)/(double)q);
			return 0;
		}
		
		/**
		 * Calculate vector similarity between two columns
		 * @param col
		 * @param col2
		 * @return similarity score between two vectors
		 */
		public double euclideanDistance(NumericVector<E> col2) {
			return distance(col2,2);
		}
		
		private NumericVector<E> filterMissing(NumericVector<E> col) {
			for(int i = 0; i < col.size(); i++) {
				if(TheoreticalValue.isTheoretical((col.get(i))))
					col.remove(i--); //Subtract to move back and reassess new position
			}
			
			col.charRep = col.asCharacter();
			return col;
		}
		
		public E innerProduct(NumericVector<E> col) {
			if(this.size() != col.size())
				throw new DimensionMismatchException("Dim mismatch");
			
			double answer = 0;
			for(int i = 0; i < col.size(); i++) {
				answer += (new Double(charRep.get(i)) * 
						  new Double(col.charRep.get(i)) );
			}
			
			return (E) (highestCommonConvertableClass(col).equals(Double.class) ? 
					convertDoubleToE(answer) : convertDoubleToInteger(answer));
		}
		
		public NumericVector<E> logTransform() {
			DataColumn dc = new DataColumn();
			for(E e : this) {
				if(TheoreticalValue.isTheoretical(e))
					dc.add(Infinite.isInfinite(e) ? new Infinite("-inf") : new MissingValue());
				else dc.add(Math.log(new Double(e.toString())));
			} return new NumericVector<E>(dc);
		}

		/**
		 * Identify maximum in column
		 * @param col
		 * @return max in column
		 */
		public E max() {
			NumericVector<E> col = new NumericVector<E>(this);
			if(this.containsNA()) { //MISSING VAL
				col = filterMissing(col);
				if(col.isEmpty())
					throw new MissingValueException("Cannot perform arithmetic operation on entirely theoretical row");
			}
			
			Collections.sort(col.charRep, compar);
			double answer = Double.valueOf(col.charRep.get(col.size()-1));
			return (E) (highestCommonConvertableClass(col).equals(Double.class) ? 
					convertDoubleToE(answer) : convertDoubleToInteger(answer));
		}
		
		/**
		 * Calculate mean of column
		 * @param col
		 * @return mean of column
		 */
		public double meanBasic() {
			NumericVector<E> col = new NumericVector<E>(this);
			if(this.containsNA()) { //MISSING VAL
				col = filterMissing(col);
				if(col.isEmpty())
					throw new MissingValueException("Cannot perform arithmetic operation on entirely theoretical row");
			}
			E sum = sumCalc();
			
			return highestCommonConvertableClass(col).equals(Integer.class) ? 
				(Double)convertIntegerToDouble((Integer)sum) / col.size() : (Double)sum / col.size();
		}
		
		/**
		 * Identify minimum in a column
		 * @param col
		 * @return min in column
		 */
		public E min() {
			NumericVector<E> col = new NumericVector<E>(this);
			if(this.containsNA()) { //MISSING VAL
				col = filterMissing(col);
				if(col.isEmpty())
					throw new MissingValueException("Cannot perform arithmetic operation on entirely theoretical row");
			}
			
			Collections.sort(col.charRep, compar);
			double answer = Double.valueOf(col.charRep.get(0));
			return (E) (highestCommonConvertableClass(col).equals(Double.class) ? 
					convertDoubleToE(answer) : convertDoubleToInteger(answer));
		}
		
		public E range() {
			E max = max();
			E min = min();
			if(content.equals(Double.class))
				return convertDoubleToE((Double)max - (Double)min);
			return convertIntegerToE((Integer)max - (Integer)min);
		}
		
		public NumericVector<Double> scaleByFactorCalc(double scalar) {
			DataColumn<String> col = (DataColumn<String>) this.clone();
			col = col.asCharacter();
			
			DataColumn newD = new DataColumn(this.size());
			newD.checkedForConvertable = true;
			newD.isNumeric = true;
			
			for(String s : col) {
				if(MissingValue.isNA(s))
					newD.add(new MissingValue());
				else if(Infinite.isInfinite(s))
					newD.add(new Infinite(s));
				else newD.add( Double.valueOf(s)*scalar );
			}
			return new NumericVector<Double>(newD);
		}
		
		/**
		 * Calculate the standard deviation of a column
		 * @param col
		 * @return standard deviation of column
		 */
		public double standardDeviationCalc() {
			return Math.sqrt(varianceCalc());
		}
		
		public NumericVector<Double> standardize() {
			double sd = standardDeviationCalc();
			NumericVector centered = center();
			DataColumn dc = new DataColumn();
			for(Object dub : centered) {
				if(TheoreticalValue.isTheoretical(dub))
					dc.add(Infinite.isInfinite(dub) ? new Infinite() : new MissingValue());
				else dc.add((Double)dub/sd);
			}
			return new NumericVector<Double>(dc);
		}
		
		/**
		 * Calculate the sum of all the elements in a column
		 * @param col
		 * @return sum of column
		 */
		public E sumCalc() {
			NumericVector<E> col = new NumericVector<E>(this);
			if(this.containsNA()) { //MISSING VAL
				col = filterMissing(col);
				if(col.isEmpty())
					throw new MissingValueException("Cannot perform arithmetic operation on entirely theoretical row");
			}
			
			double sum = 0;
			for(int i = 0; i < col.size(); i++)
				sum += new Double(col.charRep.get(i));
			return (E) (highestCommonConvertableClass(col).equals(Double.class) ? 
					convertDoubleToE(sum) : convertDoubleToInteger(sum));
		}
		
		public void summary() {
			E sum = sumCalc();
			double mean = mean();
			double std = standardDeviationCalc();
			E max = max();
			E min = min();
			System.out.println(name());
			System.out.println("Sum:\t\t\t" + sum);
			System.out.println("Mean:\t\t\t" + mean);
			System.out.println("Standard Deviation:\t" + std);
			System.out.println("Max:\t\t\t" + max);
			System.out.println("Min:\t\t\t" + min);
			System.out.println("Width:\t\t\t" + this.width());
			System.out.println("Type:\t\t\t" + content);
			System.out.println("Size:\t\t\t" + this.size());
		}
		
		/**
		 * Calculate the variance of a column
		 * @param col
		 * @return variance of column
		 */
		public double varianceCalc() {
			NumericVector<E> col = new NumericVector<E>(this);
			if(this.containsNA()) { //MISSING VAL
				col = filterMissing(col);
				if(col.isEmpty())
					throw new MissingValueException("Cannot perform arithmetic operation on entirely theoretical row");
			}
			
			double avg = meanBasic();
			double sum = 0;
			for( String o : col.charRep ) {
				sum += (new Double(o) - avg) * (new Double(o) - avg);
			}
			return sum / (col.size()-1);
		}
	}
	
	/**
	 * A faster, static inner class to handle type conversions
	 * @author Taylor G Smith
	 */
	static final class TypeConversionUtils {
		/**
		 * Will return a version of the DataColumn with all contents
		 * converted to String.
		 * @return a string-converted instance of DataColumn
		 */
		@SuppressWarnings("unchecked")
		public static <T extends Comparable<? super T> & java.io.Serializable> DataColumn<String> asCharacter(DataColumn<T> arg0) {
			Collection<String> coll = new ArrayList<String>();
			for(T t : arg0) {
				coll.add(t.toString());
			}
			DataColumn<String> data = new DataColumn<String>(coll);
			data.type = String.class;
			data = (DataColumn<String>) asCharacterUtilities(data,arg0);
			return data;
		}
		
		@SuppressWarnings({ "unchecked" })
		public static <T extends Comparable<? super T> & java.io.Serializable> DataColumn<Factor> asFactor(DataColumn<T> arg0) {
			if(arg0.contentClass().equals(Factor.class))
				return (DataColumn<Factor>) arg0;
			DataColumn<String> asString = arg0.asCharacter();
			HashMap<String,Factor> factorMap = Factor.factorMap(asString.unique());
			
			Collection<Factor> coll = new ArrayList<Factor>();
			for(String s : asString)
				coll.add(factorMap.get(s));
			DataColumn<Factor> data = new DataColumn<Factor>(coll);
			data.type = Factor.class;
			data = (DataColumn<Factor>) asCharacterUtilities(data,arg0);
			return data;
		}
		
		private static <T extends Comparable<? super T> & java.io.Serializable> DataColumn<?> asCharacterUtilities(DataColumn<?> data, DataColumn<T> arg0) {
			data.isConvertable = arg0.isConvertable;
			data.checkedForConvertable = arg0.checkedForConvertable;
			data.isNumeric = false;
			data.checkedForNumericism = false;
			data.conversionType = arg0.conversionType;
			if(arg0.widthCalculated) {
				data.width = arg0.width;
				data.widthCalculated = true;
			}
			data.name = arg0.name;
			return data;
		}
		
		/**
		 * Utilities for when a column is converted to some type of numeric. These are 
		 * all attributes the new datacolumn will possess post-conversion
		 * @param returnable
		 * @param original
		 * @return the numerically-converted DataColumn with cloned attributes
		 */
		private static <T extends Comparable<? super T> & java.io.Serializable> DataColumn<? extends Number> asNumericUtilities(DataColumn<? extends Number> returnable, DataColumn<T> original) {
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
		
		@SuppressWarnings({ "unchecked" })
		public static <T extends Comparable<? super T> & java.io.Serializable> DataColumn<Double> asDouble(DataColumn<T> arg0) {
			ArrayList collection = new ArrayList(arg0.size());
			for(T t : arg0) {
				String tar = arg0.contentClass().equals(String.class) ? (String) t : t.toString();
				if(MissingValue.isNA(tar))
					collection.add(new MissingValue());
				else if(Infinite.isInfinite(tar))
					collection.add(new Infinite(tar));
				else collection.add( new Double(tar) );
			}
			DataColumn<Double> returnable = new DataColumn<Double>(collection);
			returnable.type = Double.class;
			returnable = (DataColumn) asNumericUtilities(returnable, arg0);
			return returnable;
		}
		
		@SuppressWarnings({ "unchecked" })
		public static <T extends Comparable<? super T> & java.io.Serializable> DataColumn<Integer> asInteger(DataColumn<T> arg0) {
			/*if(arg0.contentClass().equals(Integer.class))
				return (DataColumn) arg0;*/
			ArrayList collection = new ArrayList(arg0.size());
			for(T t : arg0) {
				String tar = arg0.contentClass().equals(String.class) ? (String) t : t.toString();
				if(MissingValue.isNA(tar))
					collection.add(new MissingValue());
				else if(Infinite.isInfinite(tar))
					collection.add(new Infinite(tar));
				else collection.add( new Integer(tar) );
			}
			DataColumn<Integer> returnable = new DataColumn<Integer>(collection);
			returnable.type = Integer.class;
			returnable = (DataColumn) asNumericUtilities(returnable, arg0);
			return returnable;
		}
		
		public static <T extends Comparable<? super T> & java.io.Serializable> Class<?> contentClass(DataColumn<T> arg0) {
			if(!(null==arg0.type))
				return arg0.type;
			else if(arg0.isEmpty())
				return null;
			else {
				for(T t : arg0) {
					/* New structure means an all NA column
					 * should have a null type*/
					if(!TheoreticalValue.isTheoretical(t))
						return (arg0.type = t.getClass());
					else continue;
				}
			}
			return arg0.type;
		}
		
		public static <T extends Comparable<? super T> & java.io.Serializable> boolean isConvertableToNumeric(DataColumn<T> arg0) {
			if(arg0.isNumeric()) {
				arg0.checkedForConvertable = true;
				arg0.isConvertable = true;
				arg0.conversionType = contentClass(arg0);
				return true;
			} else if(arg0.checkedForConvertable)
				return arg0.isConvertable;
			if( numericConversionType(arg0)==null ) {
				arg0.checkedForConvertable = true;
				arg0.isConvertable = false;
				return false;
			}
			arg0.checkedForConvertable = true;
			arg0.isConvertable = true;
			return true;
		}
		
		public static <T extends Comparable<? super T> & java.io.Serializable> boolean isNumeric(DataColumn<T> arg0) {
			if(!arg0.checkedForNumericism) {
				if(arg0.isEmpty())
					return false;
				arg0.isNumeric = Number.class.isAssignableFrom(contentClass(arg0));
				arg0.checkedForNumericism = true;
				return arg0.isNumeric;
			} else return arg0.isNumeric;
		}

		public static <T extends Comparable<? super T> & java.io.Serializable> Class<?> numericConversionType(DataColumn<T> arg0) {
			if(isNumeric(arg0))							//The generic type was declared so we know it
				return arg0.type;
			else if(!(null == arg0.conversionType))		//We have already found the type
				return arg0.conversionType;
			else {										//It is a string type -- need to find conversion
				int cutoff = arg0.size() < 20 ? arg0.size() : 	//If it's too short, just loop it all
									 arg0.size()/3;				//For now let's cutoff at the halfway point to save time
				int start = 0;
				String col = ((Vector<T>)arg0).toString().toLowerCase();			//String representation of column
				boolean possibleDouble = col.contains("e") || col.contains(".");	//A double will have a . or e
				for(T t : arg0) {
					if(start++ == cutoff)				//Now we only look at a portion of the column to determine
						return arg0.conversionType;
					String ts = t.toString(); 			//String version for pattern matching...
					if( numberCouldBeInteger(ts) ) {	//Could it be an integer? Don't negate if is possibleDouble because of X.0 corner case
						arg0.conversionType = Integer.class;
					} else if( possibleDouble && numberCouldBeDouble(ts) ) {
						return (arg0.conversionType = Double.class); 			//Hierarchical. If double, automatically return double
					} else if(!TheoreticalValue.isTheoretical(t)) return null; 	//Skip out early if it isnt int, double, NA, inf, etc
				}
				return arg0.conversionType;
			}
		}
		
		private static boolean numberCouldBeInteger(String ts) {
			return ts.matches("[-+]?[0-9]+"); // << Should handle everything
		}
		
		private static boolean numberCouldBeDouble(String ts) {
			return ts.matches("[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?");
		}
	}
	
	/**
	 * Implementation of Map.Entry for use in sorting a DataTable --
	 * will couple the Object with its index for index tracking in a sort
	 * @author Taylor G Smith
	 * @param <K>
	 * @param <V>
	 */
	private class SortableEntry<K,V> implements Map.Entry<K,V> {
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
	/*--------------------------------------------------------------------*/
	
	
	/**
	 * Skip type safety check -- only called form CsvParser
	 * @param element
	 * @return true if successfully added
	 */
	protected final boolean addFromTrusted(T element) {
		return super.add(element);
	}
	
	/**
	 * Skip type safety check -- only called form CsvParser
	 * @param element
	 * @param index
	 */
	protected final void addFromTrusted(T element, int index) {
		super.add(index, element);
	}
	
	/**
	 * Adds the given object to the DataColumn
	 * @return true if successfully added
	 */
	public final boolean add(T element) {
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
	public final void add(int index, T element) {
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
	public final boolean addAll(Collection<? extends T> arg0) {
		if(!ensureTypeSafety(arg0))
			throw new SchemaMismatchException("Argument class does not match column class type");
		columnUpdate();
		return super.addAll(arg0);
	}
	
	/**
	 * Adds the given collection of objects to the DataColumn
	 * beginning at the specified index
	 */
	public final boolean addAll(int index, Collection<? extends T> arg1) {
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
	@SuppressWarnings({ "unchecked" })
	public final DataColumn<String> asCharacter() {			
		if(this.contentClass().equals(String.class))
			return (DataColumn) this;
		return TypeConversionUtils.asCharacter(this);
	}
	
	/**
	 * Protected method used in DataTable transformations
	 * @return Double-converted DataColumn
	 */
	protected final DataColumn<Double> asDouble() {
		return TypeConversionUtils.asDouble(this);
	}
	
	public final DataColumn<Factor> asFactor() {
		return TypeConversionUtils.asFactor(this);
	}
	
	/**
	 * Protected method used in DataTable transformations
	 * @return Integer-converted DataColumn
	 */
	protected final DataColumn<Integer> asInteger() {
		return TypeConversionUtils.asInteger(this);
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
			return converter.equals(Integer.class) ? asInteger() : asDouble();
		} else throw new NumberFormatException("Cannot parse column to numeric");
	}
	
	@SuppressWarnings("unchecked")
	public DataColumn<Double> center() {
		return (DataColumn<Double>) new NumericVector(this).center();
	}
	
	/**
	 * Will reset this instance to default but will retain the column name
	 */
	public final void clear() {
		columnUpdate(); //Takes care of a few
		isConvertable = false;
		isNumeric = false;
		type = null;
		conversionType = null;
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
		
		clone.widthCalculated = true;
		clone.width = width;
		clone.checkedForNumericism = true;
		clone.isNumeric = isNumeric;
		clone.checkedForConvertable = true;
		clone.isConvertable = isConvertable;
		clone.conversionType = conversionType;
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
	public final boolean containsNA() {
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
	public final Class<?> contentClass() {
		return TypeConversionUtils.contentClass(this);
	}
	
	/**
	 * Will count the number of theoretical values (NA, Infinite, NaN)
	 * within the column
	 */
	public final int countMissingValues() {
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
	private boolean ensureTypeSafety(Object element) {
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
	private boolean ensureTypeSafety(Collection<? extends T> element) {
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
	
	public boolean equals(Object o) {
		if(!(o instanceof DataColumn))
			return false;
		return o.hashCode() == this.hashCode();
	}
	
	/**
	 * Returns the Minkowski distance between the two columns, where q
	 * is the distance tuning knob.
	 * @param arg0
	 * @param q
	 * @return the Minkowski distance between two columns
	 */
	@SuppressWarnings("unchecked")
	public final double distance(DataColumn arg0, int q) {
		return new NumericVector(this).distance(new NumericVector(arg0), q);
	}
	
	/**
	 * Returns the vector similarity (or Euclidean distance) between
	 * this instance of the DataColumn and another. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @param arg0
	 * @return vector similarity
	 */
	@SuppressWarnings({ "unchecked" })
	public final double euclideanDistance(DataColumn arg0) {
		return new NumericVector(this).euclideanDistance(new NumericVector(arg0));
	}
	
	public int hashCode() {
		return super.hashCode()^12;
	}
	
	/**
	 * Returns the inner product between this instance of the DataColumn and
	 * another. NOTE: this will throw an exception for a datacolumn that is 
	 * either NOT NUMERIC or NOT CONVERTABLE to a numeric type
	 * @param arg0
	 * @return inner product
	 */
	@SuppressWarnings("unchecked")
	public final T innerProduct(DataColumn arg0) {
		return (T) new NumericVector(this).innerProduct(new NumericVector(arg0));
	}
	
	/**
	 * Returns whether the DataColumn could be parsed to a numeric type 
	 * (either Integer or Double)
	 * @return whether the DataColumn can be converted to a numeric type
	 */
	public final boolean isConvertableToNumeric() {
		return TypeConversionUtils.isConvertableToNumeric(this);
	}
	
	/**
	 * Returns whether the DataColumn is currently a 
	 * numeric type (assignable from Number.class)
	 * @return whether the column is numeric
	 */
	public final boolean isNumeric() {
		return TypeConversionUtils.isNumeric(this);
	}
	
	@SuppressWarnings("unchecked")
	public DataColumn<T> logTransform() {
		return (DataColumn<T>) new NumericVector(this).logTransform();
	}
	
	/* -- Intra-DataTable sorts -- */
	/**
	 * Sorts the actual map entries
	 * @return a comparator to sort a collection of map entries
	 */
	private Comparator<Map.Entry<T,Integer>> mapEntrySorter() {
		return new Comparator<Map.Entry<T,Integer>>() {
			@Override
			public int compare(Map.Entry<T,Integer> s1, Map.Entry<T,Integer> s2) {
				return returnInComparator(s1.getKey(),s2.getKey());
			}
		};
	}
	
	/**
	 * Returns the maximum number in the DataColumn. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @return max of column
	 */
	@SuppressWarnings("unchecked")
	public T max() {
		return (T) new NumericVector(this).max();
	}
	
	/**
	 * Returns the mean of the DataColumn. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @return mean of column
	 */
	@SuppressWarnings("unchecked")
	public final double mean() {
		return new NumericVector(this).meanBasic();
	}
	
	/**
	 * Returns the minimum number in the DataColumn. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @return min in column
	 */
	@SuppressWarnings("unchecked")
	public T min() {
		return (T) new NumericVector(this).min();
	}
	
	/**
	 * Returns the most frequently occurring object in the DataColumn. 
	 * NOTE: this will work for ANY object type, not just numeric
	 * @return mode of column
	 */
	@SuppressWarnings("unchecked")
	public T mode() {
		/* Less for it to iterate over now */
		HashSet<Object> rawOcc = new HashSet<Object>();
		rawOcc.addAll(this);
		
		HashMap<Object,Integer> hm = new HashMap<Object,Integer>();
		ArrayList<Map.Entry<Object,Integer>> list = new ArrayList<Map.Entry<Object,Integer>>();
		for(Object o : rawOcc)
			hm.put(o, Collections.frequency(this,o));
		list.addAll(hm.entrySet());
		
		/* Sort by occurrence */
		Collections.sort(list, freqCompar);
		
		int i = list.size()-1;
		T ob;
		while(TheoreticalValue.isTheoretical( ob = (T)list.get(i--).getKey() ))
			continue;
		return ob;
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
	protected final Class<?> numericConversionType() {
		return TypeConversionUtils.numericConversionType(this);
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
	public final boolean remove(Object arg0) {
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
	public final T remove(int index) {
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
	public final boolean removeAll(Collection<?> arg0) {
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
	private void render() {
		width = sortByLength();
		widthCalculated = true;
	}
	
	/**
	 * Checks for TheoreticalValues in a comparator operation and
	 * returns the appropriate value for sorting
	 * @return
	 */
	private int returnInComparator(T s1, T s2) {
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
	public final T set(int index, T element) {
		if(this.size() > 0)
			if(!ensureTypeSafety(element))
				throw new SchemaMismatchException("Element type does not match column type");
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
	
	@SuppressWarnings("unchecked")
	public T range() {
		return (T) new NumericVector(this).range();
	}
	
	public final static DataColumn readFromSerializedObject(FileInputStream fileIn) throws IOException, ClassNotFoundException {
		DataColumn d = null;
		ObjectInputStream in = new ObjectInputStream(fileIn);
		d = (DataColumn) in.readObject();
		in.close();
		fileIn.close();
		return d;
	}
	
	@SuppressWarnings("unchecked")
	public final DataColumn<Double> scaleByFactor(double scalar) {
		return (DataColumn) new NumericVector(this).scaleByFactorCalc(scalar);
	}

	/**
	 * Generate item : index list of map entries
	 * @return a list of map entries (Object : index in column)
	 */
	private List<Map.Entry<T,Integer>> sortableMapEntries() {
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
	private List<Integer> sortedMapEntryValues(List<Map.Entry<T,Integer>> sorted) {
		List<Integer> arr = new ArrayList<Integer>();
		for(Map.Entry<T,Integer> o : sorted)
			arr.add(o.getValue());
		return arr;
	}
	
	protected final List<Integer> sortedAscendingMapEntries() {
		List<Map.Entry<T,Integer>> sortables = sortableMapEntries();
		Collections.sort(sortables, mapEntrySorter());
		return sortedMapEntryValues(sortables);
	}
	
	protected final List<Integer> sortedDescendingMapEntries() {
		List<Map.Entry<T,Integer>> sortables = sortableMapEntries();
		Collections.sort(sortables, mapEntrySorter());
		Collections.reverse(sortables);
		return sortedMapEntryValues(sortables);
	}
	
	/**
	 * To be used with render() -- this sorts the column by string length
	 * @return the highest length item in the column
	 */
	private int sortByLength() {
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
	private Comparator<T> sortableComparator() {
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
	protected final void sortAscending() {
		Collections.sort(this, sortableComparator());
	}
	
	/**
	 * Sorts the DataColumn descending (NAs high)
	 */
	protected final void sortDescending() {
		Collections.sort(this, Collections.reverseOrder(sortableComparator()));
	}
	
	/**
	 * Calculates the standard deviation of the column. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @return standard deviation of column
	 */
	@SuppressWarnings("unchecked")
	public final double standardDeviation() {
		return new NumericVector(this).standardDeviationCalc();
	}
	
	@SuppressWarnings("unchecked")
	public DataColumn<Double> standardize() {
		return (DataColumn<Double>) new NumericVector(this).standardize();
	}
	
	public final DataColumn<T> subsetByCondition(SubsettableCondition sub) {
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
	
	public final boolean[] subsetLogicalVector(SubsettableCondition sub) {
		return sub.evaluate(this);
	}
	
	/**
	 * An arithmetic operation for use on a numeric-typed column.  
	 * NOTE: this will throw an exception for a datacolumn that is 
	 * either NOT NUMERIC or NOT CONVERTABLE to a numeric type
	 * @return sum of column
	 */
	@SuppressWarnings("unchecked")
	public final T sum() {
		return (T) new NumericVector(this).sumCalc();
	}
	
	/**
	 * Will print the column's descriptive statistics, if any,
	 * else if will print info regarding the column's attributes
	 */
	@SuppressWarnings("unchecked")
	public void summary() {
		if(this.isNumeric()) //If numeric
			new NumericVector(this).summary();
		else { //If not numeric
			System.out.println(this.name);
			System.out.println("Width:\t" 	+ this.width());
			System.out.println("Type:\t" 	+ this.contentClass());
			System.out.println("Size:\t" 	+ this.size());
		}
		System.out.println(System.getProperty("line.separator"));
	}
	
	/**
	 * Converts the column to an instance of DataRow
	 * @return the column parsed to DataRow
	 */
	public final DataRow toDataRow() {
		DataRow d = new DataRow(this,name);
		return d;
	}
	
	public LinkedHashSet<T> unique() {
		return new LinkedHashSet<T>(this);
	}
	
	/**
	 * Calculates the variance of the column. NOTE: this will throw
	 * an exception for a datacolumn that is either NOT NUMERIC or NOT
	 * CONVERTABLE to a numeric type
	 * @return variance of column
	 */
	@SuppressWarnings("unchecked")
	public final double variance() {
		return new NumericVector(this).varianceCalc();
	}
	
	/**
	 * The width of the longest element in the DataColumn (for spacing purposes)
	 * @return widest element in column (for print rendering)
	 */
	public final int width() {
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
