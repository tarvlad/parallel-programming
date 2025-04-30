package org.nsu.syspro.parprog.solution.profiling;

import org.nsu.syspro.parprog.external.MethodID;

public class LocalExecutionCache {
    private MethodID[] buffer;
    private final ExecutionCounters localCounters = new ExecutionCounters(true);
    private int size;

    public LocalExecutionCache(int capacity) {
        buffer = new MethodID[capacity];
    }

    public int countAndRead(MethodID id) {
        assert size < buffer.length;
        buffer[size++] = id;

        return localCounters.counter(id).countAndRead();
    }

    public void syncWithGlobalCounters(ExecutionCounters global) {
        for (var entry : global.countersRaw().entrySet()) {
            var id = entry.getKey();
            var counter = entry.getValue();
            localCounters.counter(id).setValue(counter.value());
        }
    }

    public boolean bufferFull() {
        return size == buffer.length;
    }

    public MethodID[] takeBuffer() {
        size = 0;
        var b = buffer;
        buffer = new MethodID[b.length];
        return b;
    }
}
