package org.nsu.syspro.parprog.solution.profiling;

import org.nsu.syspro.parprog.external.MethodID;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Storage for execution counters - provides
 * possibility to get counter associated with given method id performing
 * its initialization if there's no one yet presented
 */
public class ExecutionCounters {
    private final Lock countersSyncLock;
    private final Map<MethodID, ExecutionCounter> counters = new HashMap<>();

    public ExecutionCounters(boolean local) {
        if (local) {
            countersSyncLock = null;
        } else {
            countersSyncLock = new ReentrantLock(true);
        }
    }

    public ExecutionCounters() {
        this(false);
    }

    public Map<MethodID, ExecutionCounter> countersRaw() {
        return counters;
    }

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

    public void withLockedCounters(Runnable action) {
        if (countersSyncLock == null) {
            action.run();
        } else {
            countersSyncLock.lock();
            try {
                action.run();
            } finally {
                countersSyncLock.unlock();
            }
        }
    }

    public void syncWithLocalBuffer(MethodID[] buffer) {
        for (MethodID entry : buffer) {
            counter(entry).countAndRead();
        }
    }
}
