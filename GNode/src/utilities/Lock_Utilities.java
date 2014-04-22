package utilities;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Lock_Utilities {
	
	private static HashMap<String, ReentrantReadWriteLock> locks = new HashMap<>();
	private static ReentrantReadWriteLock selfRWLock = new ReentrantReadWriteLock();
	
	public static ReentrantReadWriteLock ObtainReadWriteLock(String name) {
		selfRWLock.writeLock().lock();
		if (locks.get(name) == null) {
			locks.put(name, new ReentrantReadWriteLock());
		}
		selfRWLock.writeLock().unlock();
		
		selfRWLock.readLock().lock();
		ReentrantReadWriteLock result = locks.get(name);
		selfRWLock.readLock().unlock();
		
		return result;
	}
	
	public static void removeReadWriteLock(String name) {
		selfRWLock.writeLock().lock();
		locks.remove(name);
		selfRWLock.writeLock().unlock();
	}
	
}
