package ds.bplusTree;

import java.util.Set;

public interface B<T extends Comparable<T>, V> {

	public V get(T key); // 查询

	public void remove(T key); // 移除

	public void insertOrUpdate(T key, V obj); // 插入或者更新，如果已经存在，就更新，否则插入

	public Set<T> getKeySet();
}
