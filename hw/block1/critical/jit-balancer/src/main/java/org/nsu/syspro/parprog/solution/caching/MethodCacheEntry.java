package org.nsu.syspro.parprog.solution.caching;

import org.nsu.syspro.parprog.external.CompiledMethod;

/**
 * Represents {@link MethodCache} entry, which contains completed compilation result for L1 or L2 cache.
 * Used as a result of local/global cache lookup and as local cache element
 */
public class MethodCacheEntry {
    /**
     * Entry kind. EMPTY value used to represent unsuccessful cache lookup result and
     * should not be contained in any local cache entry placed in inner map
     */
    public enum Kind {
        EMPTY,
        L1,
        L2
    }

    private final CompiledMethod method;
    private final Kind kind;

    /**
     * Creates empty entry for unsuccessful cache lookup representation
     *
     * @return constructed EMPTY cache entry
     */
    public static MethodCacheEntry forEmpty() {
        return new MethodCacheEntry(null, Kind.EMPTY);
    }

    /**
     * Creates cache entry with L1-compiled method for representing global/local
     * cache lookup result or local cache entry
     *
     * @param method L1-compiled method associated with constructed entry
     * @return constructed method cache entry with given L1-compiled method
     */
    public static MethodCacheEntry forL1Entry(CompiledMethod method) {
        return new MethodCacheEntry(method, Kind.L1);
    }

    /**
     * Creates cache entry with L2-compiled method for representing global/local
     * cache lookup result or local cache entry
     *
     * @param method L2-compiled method associated with constructed entry
     * @return constructed method cache entry with given L2-compiled method
     */
    public static MethodCacheEntry forL2Entry(CompiledMethod method) {
        return new MethodCacheEntry(method, Kind.L2);
    }

    private MethodCacheEntry(CompiledMethod method, Kind kind) {
        this.method = method;
        this.kind = kind;
    }

    /**
     * Gets compiled method used for submitting to execution engine later
     *
     * @return L1 or L2 compiled method associated with given cache entry
     */
    public CompiledMethod method() {
        return method;
    }

    /**
     * Get given cache entry kind for using it in scheduling later compilation tasks purpose
     *
     * @return given cache entry kind
     */
    public Kind kind() {
        return kind;
    }
}
