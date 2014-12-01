package coffeetable.interfaces;

public interface VectorUtilities<T> {
	public String name();
	public void print();
	public void setName(String name);
	public int countMissingValues();
	public boolean containsNA();
}
