package coffeetable.algorithms;

import coffeetable.datastructures.DataRow;
import coffeetable.math.TheoreticalValue;
import coffeetable.datastructures.Matrix;

public class MatrixImputation {
	private Matrix<? extends Number> matrix;
	
	public MatrixImputation(Matrix<? extends Number> matrix) {
		this.matrix = matrix;
	}
	
	public static <T extends Number & Comparable<? super T> & java.io.Serializable> Matrix<T> simpleImputation(Matrix<T> matrix) {
		for(DataRow row : matrix.rows()) {
			if(!row.containsNA())
				continue;
			for(int i = 0; i < row.size(); i++)
				if(TheoreticalValue.isTheoretical(row.get(i)))
					row.set(i, matrix.getColumn(i).mean());
		} return matrix;
	}
	
	public void simpleImputation() {
		if(null == matrix)
			throw new NullPointerException();
		matrix = simpleImputation(matrix);
	}
	
	public Matrix<? extends Number> matrix() {
		return matrix;
	}
}
