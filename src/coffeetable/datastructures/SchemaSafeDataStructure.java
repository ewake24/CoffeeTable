package coffeetable.datastructures;

import java.io.Serializable;

/**
 * A super, abstract class for DataTable providing all schema operations
 * @author Taylor G Smith
 */
public abstract class SchemaSafeDataStructure implements Serializable {
	private static final long serialVersionUID = -5011930758106873757L;
	protected Schema schema = null;
	
	/**
	 * Return the DataTable's schema
	 */
	public final Schema schema() {
		return schema;
	}
	
	/**
	 * Determines whether the schema of the incoming row
	 * is safe to add to the current DataTable
	 * @param sch
	 * @return whether an addition can be made
	 */
	protected final boolean schemaIsSafe(Schema sch) {
		return schema.isSafe(sch);
	}
	
	protected final void updateSchema(Class<? extends Object> appendable) {
		schema.add(appendable);
	}
	
	protected final void updateSchemaAt(int index, Class<? extends Object> appendable) {
		schema.add(index, appendable);
	}
	
	protected final void updateSchemaFromNew(Class<? extends Object> appendable) {
		if(null == schema)
			schema = new Schema();
		schema.add(appendable);
	}
	
	protected final void updateSchemaFromRemove(int index) {
		schema.remove(index);
	}
}
