package org.nsu.syspro.parprog.solution.caching;

import org.nsu.syspro.parprog.external.CompiledMethod;
import org.nsu.syspro.parprog.external.MethodID;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Thread-safe method cache with mapping of given method id to associated L1 or L2 in-progress or finished compilation,
 * provides - with same guarantees as ConcurrentHashMap - O(1) lookup by
 * given method id and O(1) put and start of new compilation session with idempotent mapping of method id to
 * compilation session across multiple threads while new entry initialization
 */
public class MethodCache {
    private final Map<MethodID, Future<CompiledMethod>> l1Cache = new ConcurrentHashMap<>();
    private final Map<MethodID, Future<CompiledMethod>> l2Cache = new ConcurrentHashMap<>();

    private static Future<CompiledMethod> newDummy() {
        return new CompletableFuture<>() {
            @Override
            public boolean isDone() {
                return false;
            }
        };
    }

    private static void putIfAbsent(MethodID id, Map<MethodID, Future<CompiledMethod>> cache, Supplier<Future<CompiledMethod>> pendingCompilation) {
        if (cache.get(id) != null) {
            return;
        }
        Future<CompiledMethod> dummy = newDummy();
        if (cache.putIfAbsent(id, dummy) == null) {
            assert cache.get(id) == dummy;
            // successfully reserved space for put
            Future<CompiledMethod> compilation = pendingCompilation.get();
            cache.put(id, compilation);
        }
    }

    /**
     * Performs thread-safe - with same guarantees as ConcurrentHashMap - put to cache of level represented
     * by {@code kind} and initialization of compilation session from {@code pendingCompilation} supplier.
     * Mapping of key to cache entry is idempotent function and initialization of new entry also have the same
     * guarantees as ConcurrentHashMap
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
}
