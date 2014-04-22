package ds.bplusTree;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BPlusTree<T extends Comparable<T>, V> implements B<T,V> {
	
	/** lock */
	ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	
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
		readWriteLock.readLock().lock();
		V rlt = root.get(key);
		readWriteLock.readLock().unlock();
		return rlt;
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
		readWriteLock.readLock().lock();
		Set<T> slt = keySet;
		readWriteLock.readLock().unlock();
		return slt;
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
	
	public void lockWrite() {
		readWriteLock.writeLock().lock();
	}
	
	public void unlockWrite() {
		readWriteLock.writeLock().unlock();
	}
	
	//测试
	public static void main(String[] args) {
//		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>(6);
//		Random random = new Random();
//		long current = System.currentTimeMillis();
//		for (int j = 0; j < 100000; j++) {
//			for (int i = 0; i < 100; i++) {
//				int randomNumber = random.nextInt(1000);
//				tree.insertOrUpdate(new Integer(randomNumber), new Integer(randomNumber));
//			}
//
//			for (int i = 0; i < 100; i++) {
//				int randomNumber = random.nextInt(1000);
//				//tree.remove(randomNumber);
//			}
//		}
//
//		long duration = System.currentTimeMillis() - current;
//		System.out.println("time elpsed for duration: " + duration);
//		int search = 80;
//		System.out.print(tree.get(search));
		
		final BPlusTree<String, String> tree = new BPlusTree<String, String>(6);
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				if (tree.get("1")==null)
				tree.insertOrUpdate("1", "r11");
				if (tree.get("2")==null)
				tree.insertOrUpdate("2", "r12");
				if (tree.get("3")==null)
				tree.insertOrUpdate("3", "r13");
			}
		}).start();
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				if (tree.get("1")==null)
				tree.insertOrUpdate("1", "r21");
				if (tree.get("2")==null)
				tree.insertOrUpdate("2", "r22");
				if (tree.get("3")==null)
				tree.insertOrUpdate("3", "r23");
			}
		}).start();
		
		//System.out.println("Fin!");
		
	}

}
