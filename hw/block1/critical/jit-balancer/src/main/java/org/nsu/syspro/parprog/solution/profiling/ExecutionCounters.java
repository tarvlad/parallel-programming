package org.nsu.syspro.parprog.solution.profiling;

import org.nsu.syspro.parprog.external.MethodID;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Storage for execution counters - with same guarantees as {@link ConcurrentHashMap} - provides
 * possibility to get counter associated with given method id performing
 * its thread-safe initialization if there's no one yet presented
 */
public class ExecutionCounters {
    private final ConcurrentHashMap<MethodID, ExecutionCounter> counters = new ConcurrentHashMap<>();

    /**
     * Performs mapping of given method id to associated execution counter. Mapping function is idempotent
     *
     * @param id source to map into execution counter
     * @return execution counter associated with given method id
     */
    public ExecutionCounter counter(MethodID id) {
        ExecutionCounter counter = counters.get(id);
        if (counter == null) {
            ExecutionCounter newCounter = new ExecutionCounter();

            ExecutionCounter cachedCounter = counters.putIfAbsent(id, newCounter);
            counter = Objects.requireNonNullElse(cachedCounter, newCounter);
        }
        return counter;
    }
}
