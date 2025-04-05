package org.nsu.syspro.parprog.solution.caching;

import org.nsu.syspro.parprog.external.MethodID;

import java.util.HashMap;
import java.util.Map;

/**
 * Method cache with O(1) method lookup and put time.
 */
public class LocalMethodCache {
    private final Map<MethodID, MethodCacheEntry> cache = new HashMap<>();

    /**
     * Performs lookup of compiled L1 or L2 methods associated with given {@code id}
     *
     * @param id method id to search cache entry
     * @return method cache entry which contains available for current thread L1 or L2 compiled methods for given id, or empty record
     */
    public MethodCacheEntry lookup(MethodID id) {
        MethodCacheEntry entry = cache.get(id);
        if (entry == null) {
            return MethodCacheEntry.forEmpty();
        }
        return entry;
    }

    /**
     * Put given cache entry to local cache
     *
     * @param id method id for which compiled methods can be found in entry
     * @param entry compiled methods associated with given id
     */
    public void put(MethodID id, MethodCacheEntry entry) {
        cache.put(id, entry);
    }
}
