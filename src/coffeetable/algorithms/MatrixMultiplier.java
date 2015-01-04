package coffeetable.algorithms;

import java.util.TreeMap;

import coffeetable.datastructures.DataColumn;
import coffeetable.datastructures.DataRow;
import coffeetable.utils.MatrixViabilityException;
import coffeetable.datastructures.Matrix;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MatrixMultiplier {
	private final Matrix<? extends Number> arg0;
	private final Matrix<? extends Number> arg1;
	
	public MatrixMultiplier(Matrix<? extends Number> arg0, Matrix<? extends Number> arg1) {
		if(!checkViability(arg0, arg1))
			throw new MatrixViabilityException();
		this.arg0 = arg0;
		this.arg1 = arg1;
	}
	
	public final static boolean checkViability(Matrix arg0, Matrix arg1) {
		return arg0.ncol() == arg1.nrow() && !arg0.containsNA() && !arg1.containsNA();
	}
	
	public static Matrix multiply(Matrix arg0, Matrix arg1) {
		if(!checkViability(arg0, arg1))
			throw new MatrixViabilityException();
		
		int outDims = arg0.nrow();
		TreeMap<Integer, DataColumn> mapOut = new TreeMap<Integer, DataColumn>();
		for(int i=0; i < outDims; i++)
			mapOut.put(i, new DataColumn("Col"+Integer.valueOf(i+1).toString()));
		
		for(Object row : arg0.rows()) {
			int colIdx = 0;
			DataColumn leftTmp = ((DataRow)row).toDataColumn();
			
			for(Object column : arg1.columns())
				mapOut.get(colIdx++).add( leftTmp.innerProduct((DataColumn)column) );
		}
		return new Matrix(mapOut.values());
	}
	
	public Matrix<? extends Number> multiply() {
		if(null == arg0 || null == arg1)
			throw new NullPointerException();
		return multiply(arg0, arg1);
	}
}
