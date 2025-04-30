package org.nsu.syspro.parprog.solution.caching;

import org.nsu.syspro.parprog.external.CompiledMethod;
import org.nsu.syspro.parprog.external.MethodID;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Method cache with mapping of given method id to associated L1 or L2 in-progress or finished compilation
 */
public class MethodCache {
    private final Lock cacheSyncLock = new ReentrantLock(true);
    private final Map<MethodID, Future<CompiledMethod>> l1Cache = new HashMap<>();
    private final Map<MethodID, Future<CompiledMethod>> l2Cache = new HashMap<>();

    private static void putIfAbsent(MethodID id, Map<MethodID, Future<CompiledMethod>> cache, Supplier<Future<CompiledMethod>> pendingCompilation) {
        if (cache.get(id) != null) {
            return;
        }
        cache.put(id, pendingCompilation.get());
    }

    /**
     * Performs put to cache of level represented
     * by {@code kind} and initialization of compilation session from {@code pendingCompilation} supplier
     *
     * @param key key to perform mapping into compilation session
     * @param kind cache level to work with
     * @param pendingCompilation supplier which starts compilation session for given method
     */
    public void putIfAbsent(MethodID key, MethodCacheEntry.Kind kind, Supplier<Future<CompiledMethod>> pendingCompilation) {
        switch (kind) {
            case L1: putIfAbsent(key, l1Cache, pendingCompilation); return;
            case L2: putIfAbsent(key, l2Cache, pendingCompilation); return;
            default:
                throw new IllegalArgumentException("Unexpected kind " + kind);
        }
    }

    /**
     * Waiting while started compilation for level {@code kind} associated with given method with id {@code key}
     * finishes and returns compiled method
     *
     * @param key method id associated with method, which compilation need to wait
     * @param kind level of optimization to choose cache to lookup
     * @return finished compilation task result - compiled method
     */
    public CompiledMethod ensureCompiled(MethodID key, MethodCacheEntry.Kind kind) {
        Future<CompiledMethod> inProgress = get(key, kind);
        assert inProgress != null;
        try {
            return inProgress.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Future<CompiledMethod> get(MethodID key, MethodCacheEntry.Kind kind) {
        switch (kind) {
            case L1: return l1Cache.get(key);
            case L2: return l2Cache.get(key);
            default:
                throw new IllegalArgumentException("Unexpected kind " + kind);
        }
    }

    /**
     * Performs thread-safe - with same guarantees as ConcurrentHashMap - lookup for compiled methods
     * associated with given method id. Method chose in priority order L2 -> L1. If there's no yet finished
     * compilation sessions associated with given method id, then empty method cache entry returned.
     *
     * @param key method id for lookup
     * @return method cache entry with founded compiled L1 or L2 method or empty
     */
    public MethodCacheEntry lookup(MethodID key) {
        try {
            Future<CompiledMethod> l2Lookup = get(key, MethodCacheEntry.Kind.L2);
            if (l2Lookup != null && l2Lookup.isDone()) {
                return MethodCacheEntry.forL2Entry(l2Lookup.get());
            }

            Future<CompiledMethod> l1Lookup = get(key, MethodCacheEntry.Kind.L1);
            if (l1Lookup != null && l1Lookup.isDone()) {
                return MethodCacheEntry.forL1Entry(l1Lookup.get());
            }

            return MethodCacheEntry.forEmpty();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void cacheLockedAction(Runnable action) {
        cacheSyncLock.lock();
        try {
            action.run();
        } finally {
            cacheSyncLock.unlock();
        }
    }
}
