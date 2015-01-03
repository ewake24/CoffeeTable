package coffeetable.interfaces;

import coffeetable.datastructures.DataColumn;
import coffeetable.datastructures.DataRow;

public interface NumericEnforcementUtils {
	public boolean ensureColumnNumericism(DataColumn<?> col);
	public boolean ensureRowNumericism(DataRow row);
	public boolean ensureClassNumericism(Class<?> c);
}
