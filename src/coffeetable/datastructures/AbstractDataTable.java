package coffeetable.datastructures;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Abstract class implementing subsetting methods
 * @author Taylor G Smith
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractDataTable extends RenderableSchemaSafeDataStructure {
	private static final long serialVersionUID = 558389814813582961L;

	public AbstractDataTable() { 
		super(); 
	}
	
	public AbstractDataTable(int numRows, int numCols) {
		super(numRows, numCols);
	}
	
	/**
	 * Class to handle all block removals
	 * @author Taylor G Smith
	 */
	final static class BatchModUtilities {
		static boolean inRange_col(AbstractDataTable dt, int bottom, int top, boolean inclusive) {
			return inclusive ? (bottom >= 0) && (top <= dt.columns().size()-1) && (bottom <= top) :
						(bottom >= -1) && (top <= dt.columns().size()) && (bottom < top) && (top-bottom>1); //Must be at least one between them
		}
		
		static boolean inRange_row(AbstractDataTable dt, int bottom, int top, boolean inclusive) {
			return inclusive ? (bottom >= 0) && (top <= dt.rows().size()-1) && (bottom <= top) :
						(bottom >= -1) && (top <= dt.rows().size()) && (bottom < top) && (top-bottom>1); //Must be at least one between them
		}
		
		static Collection<DataColumn> removeColumnRange(AbstractDataTable dt, int lo, int hi, boolean inclusive) {
			if(!inRange_col(dt,lo,hi,inclusive))
				throw new IllegalArgumentException("Out of range");
			int colsToRemove = inclusive ? ((hi+1)-(lo+1))+1 : ((hi+1)-(lo+1))-1;
			int index = 0;
			ArrayList<DataColumn> arr = new ArrayList<DataColumn>();
			while(index++ < colsToRemove) {
				arr.add(dt.removeColumn(inclusive ? lo : lo+1));
			}
			return arr;
		}
		
		static Collection<DataRow> removeRowRange(AbstractDataTable dt, int lo, int hi, boolean inclusive) {
			if(!inRange_row(dt,lo,hi,inclusive))
				throw new IllegalArgumentException("Out of range");
			int rowsToRemove = inclusive ? ((hi+1)-(lo+1))+1 : ((hi+1)-(lo+1))-1;
			int index = 0;
			ArrayList<DataRow> arr = new ArrayList<DataRow>();
			while(index++ < rowsToRemove) {
				arr.add(dt.removeRow(inclusive ? lo : lo+1));
			}
			return arr;
		}
	}
	
	/**
	 * Handles all table segmentation (i.e., dicing operations)
	 * @author Taylor G Smith
	 */
	final class SubTable {
		private final AbstractDataTable dt;
		private AbstractDataTable newdata;
		private final int colStart;
		private final int colEnd;
		private final int rowStart;
		private final int rowEnd;
		
		public SubTable( AbstractDataTable dt, int rowStart, int rowEnd, int colStart, int colEnd ) {
			this.dt = dt;
			this.newdata = dt;
			this.colStart = colStart;
			this.colEnd = colEnd;
			this.rowStart = rowStart;
			this.rowEnd = rowEnd;
		}
		
		private AbstractDataTable cutCols(AbstractDataTable newdata) {
			if(!(colStart==0)) {
				newdata.removeColumnRange(0, colStart-1, true);
				newdata.removeColumnRange(1 + colEnd-colStart, newdata.ncol()-1, true);
			} else {
				newdata.removeColumnRange(colEnd+1, newdata.ncol()-1, true);
			} return newdata;
		}
		
		private AbstractDataTable cutRows(AbstractDataTable newdata) {
			if(!(rowStart == 0)) {
				newdata.removeRowRange(0, rowStart - 1, true);
				newdata.removeRowRange(1 + rowEnd-rowStart, newdata.nrow()-1, true);
			} else {
				newdata.removeRowRange(rowEnd+1, newdata.nrow()-1, true);
			} return newdata;
		}
		
		private boolean diceWithAllColumns() {
			return (colStart==0 && colEnd==dt.columns().size()-1);
		}
		
		private boolean diceWithAllRows() {
			return (rowStart==0 && rowEnd==dt.rows().size()-1);
		}
		
		public AbstractDataTable dice() {
			boolean diceWithAllColumns = diceWithAllColumns();
			boolean diceWithAllRows = diceWithAllRows();
			
			/* If there is nothing to cut out, just return DT */
			if(diceWithAllColumns && diceWithAllRows)
				return dt;
			if(!diceWithAllRows) 
				newdata = cutRows(newdata);
			if(!diceWithAllColumns)
				newdata = cutCols(newdata);
			return newdata;
		}
	}
	
	/**
	 * Dices the AbstractDataTable into a subset. Must be overridden with a cast:
	 * <tt>return (YourSubClass) super.dice(rowStart,rowEnd,colStart,colEnd);</tt>
	 * 
	 * @param rowStart - row from which to begin subset (inclusive)
	 * @param rowEnd - row at which to end subset (inclusive)
	 * @param colStart - col at which to begin subset (inclusive)
	 * @param colEnd - col at which to end subset (inclusive)
	 * @return a copy of the current instance of DataTable diced at the given boundaries
	 */
	public AbstractDataTable dice(int rowStart, int rowEnd, int colStart, int colEnd) {
		return new SubTable(this, rowStart, rowEnd, colStart, colEnd).dice();
	}

	
	/**
	 * Remove the columns between the lo and hi indices
	 * @param lo - the index at which to begin removal
	 * @param hi - the index at which to end removal
	 * @param inclusive - whether the lo/hi params are inclusive
	 */
	public final Collection<DataColumn> removeColumnRange(int lo, int hi, boolean inclusive) {
		return BatchModUtilities.removeColumnRange(this, lo, hi, inclusive);
	}
	
	/**
	 * Remove the rows between the lo and hi indices
	 * @param lo - the index at which to begin removal
	 * @param hi - the index at which to end removal
	 * @param inclusive - whether the lo/hi params are inclusive
	 */
	public final Collection<DataRow> removeRowRange(int lo, int hi, boolean inclusive) {
		return BatchModUtilities.removeRowRange(this, lo, hi, inclusive);
	}
}
