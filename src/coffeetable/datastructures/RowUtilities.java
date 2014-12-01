package coffeetable.datastructures;

import java.util.LinkedList;

public interface RowUtilities {
	public LinkedList<Class<?>> schema();
	public boolean schemaIsNumeric();
}
