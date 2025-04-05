package org.nsu.syspro.parprog.solution.profiling;

import org.nsu.syspro.parprog.external.MethodID;

import java.util.HashMap;
import java.util.Map;

/**
 * Storage for execution counters - provides
 * possibility to get counter associated with given method id performing
 * its initialization if there's no one yet presented
 */
public class ExecutionCounters {
    private final Map<MethodID, ExecutionCounter> counters = new HashMap<>();

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
            counters.put(id, newCounter);
            counter = newCounter;
        }
        return counter;
    }
}
