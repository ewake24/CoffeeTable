package coffeetable.interfaces;

import java.util.Set;

public interface VectorUtilities<T> {
	public String name();
	public void print();
	public void setName(String name);
	public int countMissingValues();
	public boolean containsNA();
	public Set<?> unique();
}
