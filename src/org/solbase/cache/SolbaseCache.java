package org.solbase.cache;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.rubyeye.xmemcached.exception.MemcachedException;

import org.apache.log4j.Logger;
import org.solbase.lucenehbase.IndexWriter;

public class SolbaseCache<K extends Serializable & Comparable<?>, V extends Serializable, Z extends Serializable, M extends Serializable> {

	private final static Logger logger = Logger.getLogger(SolbaseCache.class);
	private static final int BACKGROUND_THREAD_POOL_SIZE = 50;

	public static enum ModificationType {
		UPDATE, ADD, DELETE
	}

	private CachedObjectLoader<K, V, Z, M> _loader = null;
	private Long cacheTimeout;
	private VersionedCache<K, V, Z> cache;
	private VersionedCache<K, V, Z> threadLocalCache;
	
	private ThreadPoolExecutor executor = new ThreadPoolExecutor(BACKGROUND_THREAD_POOL_SIZE, BACKGROUND_THREAD_POOL_SIZE, 5, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(10));

	private class LockedResult {
		ReentrantLock lock = new ReentrantLock();
		CachedObjectWrapper<V, Z> result = null;
		int refCount = 0;
	}

	private HashMap<K, LockedResult> currentlyAccesingMap = new HashMap<K, LockedResult>();

	public SolbaseCache(Long cacheTimeout, VersionedCache<K, V, Z> cache, VersionedCache<K, V, Z> threadLocalCache) {
		this.cache = cache;
		this.threadLocalCache = threadLocalCache;
		cache.setTimeout(cacheTimeout);
		this.cacheTimeout = cacheTimeout;
	}

	public SolbaseCache(CachedObjectLoader<K, V, Z, M> loader, Long cacheTimeout, VersionedCache<K, V, Z> cache, VersionedCache<K, V, Z> threadLocalCache) {
		this(cacheTimeout, cache, threadLocalCache);
		this._loader = loader;
	}

	public CachedObjectWrapper<V, Z> getCachedObject(K key, String indexName, int start, int end) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		return getCachedObject(key, _loader, indexName, start, end);
	}

	public CachedObjectWrapper<V, Z> getCachedObject(K key, CachedObjectLoader<K, V, Z, M> loader, String indexName, int start, int end) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		// let's see if we have a data in thread local cache first if so return it
		CachedObjectWrapper<V, Z> tmp = this.threadLocalCache.get(key);
		
		if (tmp != null) {
			return tmp;
		}
		
		LockedResult lockedResult;
		synchronized (currentlyAccesingMap) {
			lockedResult = currentlyAccesingMap.get(key);
			if (lockedResult == null) {
				lockedResult = new LockedResult();
				currentlyAccesingMap.put(key, lockedResult);
			}
			lockedResult.refCount++;
		}
		try {
			tmp = this.cache.get(key);
			if (tmp == null) {
				lockedResult.lock.lock();
				try {
					if (lockedResult.result == null) {
						// other thread hasn't fetched data from hbase, so I'm going to hit hbase now
						tmp = loader.loadObject(key, start, end, this);

						if(tmp != null){
							tmp.setCacheTime(System.currentTimeMillis());
							lockedResult.result = tmp;
						} else {
							logger.debug("empty object loaded for this key: " + key.toString());
						}
					} else {
						// other thread has finished fetching from database, so return a restul from other thread
						tmp = lockedResult.result;
					}
					
					if (tmp != null) {
						// put it into cache
						this.cache.put(key, tmp);
					}
				} finally {
					lockedResult.lock.unlock();
				}
			} else {
				long currentTime = System.currentTimeMillis();
				long cacheTime = tmp.getCacheTime();
				long diff = currentTime - cacheTime;
				if (cacheTime != -1l && diff > cacheTimeout) {
					tmp.setCacheTime(-1l);
					checkAndReloadCachedObjectAsynch(this.cache, cacheTime, tmp, key, loader, currentTime, indexName, start, end, lockedResult, true);
				}
			}

			// put object in thread local cache so it doesn't have to go thru above logic again within single request
			this.threadLocalCache.put(key, tmp);
			
			return tmp;
		} finally {
			synchronized (currentlyAccesingMap) {
				lockedResult.refCount--;
				if (lockedResult.refCount == 0) {
					currentlyAccesingMap.remove(key);
				}
			}
		}

	}

	public void updateCachedObject(K key, M modificationData, String indexName, IndexWriter writer, ModificationType modType, boolean updateStore, int startDocId, int endDocId) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		updateCachedObject(key, modificationData, _loader, indexName, writer, modType, updateStore, startDocId, endDocId);
	}

	public void updateCachedObject(K key, M modificationData, CachedObjectLoader<K, V, Z, M> loader, String indexName, IndexWriter writer, ModificationType modType, boolean updateStore, int startDocId, int endDocId) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		LockedResult lockedResult;
		synchronized (currentlyAccesingMap) {
			lockedResult = currentlyAccesingMap.get(key);
			if (lockedResult == null) {
				lockedResult = new LockedResult();
				currentlyAccesingMap.put(key, lockedResult);
			}
			lockedResult.refCount++;
		}
		try {
			CachedObjectWrapper<V, Z> tmp = null;

			tmp = this.cache.get(key);
			if (tmp == null) {
				lockedResult.lock.lock();
				try {
					if (lockedResult.result != null) {
						// are we good without locking CompactedTermDocMetadata?
						tmp = lockedResult.result;
						loader.updateObject(tmp, modificationData, this, modType, startDocId, endDocId);
					}
					if (updateStore) {
						loader.updateObjectStore(key, modificationData, writer, this, modType, startDocId, endDocId);
					}

				} finally {
					lockedResult.lock.unlock();
				}
			} else {
				loader.acquireObjectLock(key);
				try {
					lockedResult.lock.lock();
					try {
						if (lockedResult.result != null) {
							tmp = lockedResult.result;
						}
						loader.updateObject(tmp, modificationData, this, modType, startDocId, endDocId);

						if (updateStore) {
							loader.updateObjectStore(key, modificationData, writer, this, modType, startDocId, endDocId);
						}
					} finally {
						lockedResult.lock.unlock();
					}
				} finally {
					loader.releaseObjectLock(key);
				}
			}
		} finally {
			synchronized (currentlyAccesingMap) {
				lockedResult.refCount--;
				if (lockedResult.refCount == 0) {
					currentlyAccesingMap.remove(key);
				}
			}
		}

	}

	private void modifyObjectRecursively(K key, M modificationData, Iterator<VersionedCache<K, V, Z>> cacheIterator, CachedObjectLoader<K, V, Z, M> loader, long currentTime, String indexName, LockedResult lockedResult, IndexWriter writer, ModificationType modType, boolean updateStore, int startDocId, int endDocId) throws IOException {

	}

	private CachedObjectWrapper<V, Z> refreshObject(VersionedCache<K, V, Z> vc, long currentCacheTime, final CachedObjectWrapper<V, Z> objectWrapper, K key, CachedObjectLoader<K, V, Z, M> loader, long currentTime, String indexName, int start, int end, LockedResult lockedResult, boolean loadFromStore) throws IOException {
		CachedObjectWrapper<V, Z> tmp = null;
		lockedResult.lock.lock();
		try {
			if (lockedResult.result == null) {
				tmp = loader.loadObject(key, start, end, this);
				tmp.setCacheTime(System.currentTimeMillis());
				lockedResult.result = tmp;
			} else {
				tmp = lockedResult.result;
			}
		} finally {
			lockedResult.lock.unlock();
		}
		
		vc.put(key, tmp);
		
		return tmp;
	}

	private void checkAndReloadCachedObjectAsynch(final VersionedCache<K, V, Z> vc, final long currentCacheTime, final CachedObjectWrapper<V, Z> objectWrapper, final K key, final CachedObjectLoader<K, V, Z, M> loader, final long currentTime, final String indexName, final int start, final int end, final LockedResult lockedResult, final boolean loadFromStore) throws IOException {

		Runnable runnable = new Runnable() {

			@Override
			public void run() {
				Z loadedIdentifier;
				try {
					loadedIdentifier = loader.getVersionIdentifier(key, start, end);
					Z versionIdentifier = objectWrapper.getVersionIdentifier();

					if (loadedIdentifier == null || versionIdentifier == null || (!versionIdentifier.equals(loadedIdentifier))) {
						refreshObject(vc, currentCacheTime, objectWrapper, key, loader, currentTime, indexName, start, end, lockedResult, loadFromStore);
					} else {
						resetCacheTime(key);
					}
				} catch (Throwable e) {
					logger.error(e);
				}

			}

		};
		try {
			executor.execute(runnable);
		} catch (RejectedExecutionException ree) {
			logger.warn("Async check and reload cached object failed, ignoring ", ree);
		}

	}

	public void resetCacheTime(K key) throws IOException {
		this.cache.resetCacheTime(key, System.currentTimeMillis());
	}

	public boolean isCacheFull() {
		return cache.isCacheFull();
	}
}
