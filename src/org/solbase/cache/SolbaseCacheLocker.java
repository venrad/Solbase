package org.solbase.cache;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.index.Term;

public class SolbaseCacheLocker {
	protected HashMap<String, LockWithRefCount> currentlyAccesingMap = new HashMap<String, LockWithRefCount>();
	
	private class LockWithRefCount {
		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		int refCount = 0;
	}
	
	public void acquireLock(Set<Term> terms, boolean read){
		for (Term term : (Set<Term>) terms) {
			String termStr = term.toString();
			LockWithRefCount lock;
			synchronized (currentlyAccesingMap) {
				lock = currentlyAccesingMap.get(termStr);
				if (lock == null) {
					lock = new LockWithRefCount();
					currentlyAccesingMap.put(termStr, lock);
				}
				lock.refCount++;
			}

			if (read) {
				lock.lock.readLock().lock();
			} else {
				lock.lock.writeLock().lock();
			}
		}
	}
	
	public void releaseLock(Set<Term> terms, boolean read) {		
		for (Term term : (Set<Term>) terms) {
			String termStr = term.toString();
			
			LockWithRefCount lock = currentlyAccesingMap.get(termStr);
			
			if (read) {
				lock.lock.readLock().unlock();
			} else {
				lock.lock.writeLock().unlock();
			}
			
			synchronized (currentlyAccesingMap) {
				lock.refCount--;
				if (lock.refCount == 0) {
					currentlyAccesingMap.remove(termStr);
				}
			}
		}
	}
}
