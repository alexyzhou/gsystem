package ds.bplusTree;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class BPlusTree<T extends Comparable<T>, V> implements B<T,V> {
	
	/** 根节点 */
	protected Node<T, V> root;
	
	/** 阶数，M值 */
	protected int order;
	
	/** 叶子节点的链表头*/
	protected Node<T, V> head;
	
	protected HashSet<T> keySet;
	
	public Node<T, V> getHead() {
		return head;
	}

	public void setHead(Node<T, V> head) {
		this.head = head;
	}

	public Node<T, V> getRoot() {
		return root;
	}

	public void setRoot(Node<T, V> root) {
		this.root = root;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	@Override
	public V get(T key) {
		return root.get(key);
	}

	@Override
	public void remove(T key) {
		root.remove(key, this);
		keySet.remove(key);
	}

	@Override
	public void insertOrUpdate(T key, V obj) {
		root.insertOrUpdate(key, obj, this);
		keySet.add(key);
	}
	
	@Override
	public Set<T> getKeySet() {
		return keySet;
	}
	
	public BPlusTree(int order){
		if (order < 3) {
			System.out.print("order must be greater than 2");
			System.exit(0);
		}
		this.keySet = new HashSet<T>();
		this.order = order;
		root = new Node<T, V>(true, true);
		head = root;
	}
	
	//测试
	public static void main(String[] args) {
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>(6);
		Random random = new Random();
		long current = System.currentTimeMillis();
		for (int j = 0; j < 100000; j++) {
			for (int i = 0; i < 100; i++) {
				int randomNumber = random.nextInt(1000);
				tree.insertOrUpdate(new Integer(randomNumber), new Integer(randomNumber));
			}

			for (int i = 0; i < 100; i++) {
				int randomNumber = random.nextInt(1000);
				//tree.remove(randomNumber);
			}
		}

		long duration = System.currentTimeMillis() - current;
		System.out.println("time elpsed for duration: " + duration);
		int search = 80;
		System.out.print(tree.get(search));
	}

}
