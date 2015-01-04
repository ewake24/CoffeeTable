package coffeetable.datastructures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;


import coffeetable.interfaces.NumericEnforcementUtils;
import coffeetable.utils.MatrixViabilityException;

/**
 * An extended class of AbstractDataTable that enforces
 * only numeric adds and will throw runtime exceptions if any adds violate
 * type safety expectations.
 * 
 * @author Taylor G Smith
 * @param <T> - extends Number & Comparable & Serializable
 * @see AbstractDataTable
 * @see RenderableSchemaSafeDataStructure
 * @see SchemaSafeDataStructure
 */
@SuppressWarnings({"rawtypes","unchecked"})
public class Matrix<T extends Number & Comparable<? super T> & java.io.Serializable> 
		extends AbstractDataTable 
		implements java.io.Serializable, NumericEnforcementUtils {
	private static final long serialVersionUID = -3452477531032967374L;
	private final static MatrixViabilityException mve = new MatrixViabilityException("Matrix must contain only numeric types");
	
	public Matrix() {
		super();
		initialize();
	}
	
	public Matrix(Collection<DataColumn<T>> cols) {
		this();
		for(DataColumn<T> dc : cols)
			super.addColumn(dc);
	}
	
	/**
	 * Be default, don't want names to be printed on the Matrix.
	 */
	private void initialize() {
		super.setOptions("print.col.names", 0);
		super.setOptions("print.table.name",0);
	}
	
	public final boolean ensureColumnNumericism(DataColumn col) {
		return ensureClassNumericism(col.contentClass());
	}
	
	public final boolean ensureRowNumericism(DataRow row) {
		return row.schema().isSingular() && row.schema().isNumeric();
	}
	
	public final boolean ensureClassNumericism(Class<?> c) {
		return Number.class.isAssignableFrom(c);
	}
	
	public final void addAllColumns(Collection<DataColumn> cols) {
		for(DataColumn col : cols)
			this.addColumn(col);
	}
	
	public final void addAllRows(Collection<DataRow> rows) {
		for(DataRow row : rows)
			this.addRow(row);
	}
	
	public final void addColumn(DataColumn<?> col) {
		if(!ensureColumnNumericism(col))
			throw mve;
		super.addColumn(col);
	}
	
	public final void addColumn(int index, DataColumn<?> col) {
		if(!ensureColumnNumericism(col))
			throw mve;
		super.addColumn(index, col);
	}
	
	public final void addRow(DataRow row) {
		if(!ensureRowNumericism(row))
			throw mve;
		super.addRow(row);
	}
	
	public final void addRow(int index, DataRow row) {
		if(!ensureRowNumericism(row))
			throw mve;
		super.addRow(index, row);
	}
	
	/**
	 * Creates an identity matrix of a given dimension
	 * @param dim - the dimensions of the identity matrix
	 * @return identity matrix of <tt>dim</tt> height and width
	 */
	public static Matrix<Integer> identityMatrix(int dim) {
		if(dim < 2)
			throw new MatrixViabilityException("Dimensions must be at least 2x2");
		ArrayList<DataColumn<Integer>> intarr = new ArrayList<DataColumn<Integer>>(dim);
		for(int i = 0; i < dim; i++) {
			Integer[] arr = new Integer[dim];
			for(int j=0; j < dim; j++)
				arr[j] = j == i ? 1 : 0;
			intarr.add(new DataColumn<Integer>(new ArrayList<Integer>(Arrays.asList(arr))));
		} return new Matrix<Integer>(intarr);
	}
	
	public static <T extends Number & Comparable<? super T> & java.io.Serializable> boolean isDiagonal(Matrix<T> mat) {
		if(!isSquare(mat))
			return false;
		for(int i = 0; i < mat.ncol(); i++)
			if(!(Double.valueOf(mat.getColumn(i).get(i).toString())!=0 
				&& Collections.frequency(mat.getColumn(i), 0)==mat.nrow()-1))
				return false;
		return true;
	}
	
	public static <T extends Number & Comparable<? super T> & java.io.Serializable> boolean isSquare(Matrix<T> mat) {
		return mat.nrow() == mat.ncol();
	}
	
	public final Object set(int rowIndex, int colIndex, Object o) {
		if(!ensureClassNumericism(o.getClass()))
			throw mve;
		return super.set(rowIndex, colIndex, o);
	}
	
	public final DataColumn<T> setColumn(int index, DataColumn col) {
		if(!ensureColumnNumericism(col))
			throw mve;
		return super.setColumn(index, col);
	}
	
	public final DataRow setRow(int index, DataRow row) {
		if(!ensureRowNumericism(row))
			throw mve;
		return super.setRow(index, row);
	}
	
	public Matrix<T> standardize() {
		Matrix<T> m = new Matrix<T>();
		for(DataColumn<T> dc : columns())
			m.addColumn(dc.standardize());
		return m;
	}
	
	public void transpose() {
		ArrayList<DataColumn<T>> dcs = new ArrayList<DataColumn<T>>();
		for(DataRow row : rows())
			dcs.add(row.toDataColumn());
		super.clear();
		
		for(DataColumn<T> dc : dcs)
			super.addColumn(dc);
		
		super.clearColumnCaches();
		super.renderState();
	}
}
