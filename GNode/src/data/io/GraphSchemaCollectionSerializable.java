package data.io;

import java.io.Serializable;
import java.util.HashMap;

public class GraphSchemaCollectionSerializable implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2586633329479755348L;

	public int version_num;
	public HashMap<String, Graph_Schema> schemas;

	public GraphSchemaCollectionSerializable() {
		version_num = 0;
		schemas = new HashMap<String, Graph_Schema>();
	}

	public GraphSchemaCollectionSerializable(int version,HashMap<String, Graph_Schema> data) {
		version_num = version;
		schemas = data;
	}
}
