package ds;

import java.util.LinkedHashMap;

public class LRULinkedHashMap<K, V> extends LinkedHashMap<K, V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7704008854487170460L;

	private int maxCapacity;//max amount of key-value pairs

	private static final float DEFAULT_LOAD_FACTOR = 0.75f;

	public LRULinkedHashMap(int maxCapacity) {
		super(maxCapacity, DEFAULT_LOAD_FACTOR, true);
		this.maxCapacity = maxCapacity;
	}

	@Override
	protected boolean removeEldestEntry(java.util.Map.Entry eldest) {
		// TODO Auto-generated method stub
		return size() > this.maxCapacity;
	}
	
}
