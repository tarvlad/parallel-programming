package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.external.ExecutionEngine;
import org.nsu.syspro.parprog.solution.caching.MethodCache;
import org.nsu.syspro.parprog.solution.jit.JitEngine;
import org.nsu.syspro.parprog.solution.profiling.ExecutionCounters;

/**
 * Context of the current scheduling session, contains global method cache,
 * execution counters and engine for submitting methods compilation tasks using bounded number of jit threads
 */
public class SessionContext {
    private final ExecutionEngine executor;
    private final MethodCache cache;
    private final ExecutionCounters counters;
    private final JitEngine jitEngine;

    public SessionContext(ExecutionEngine executor, MethodCache cache, ExecutionCounters counters, JitEngine jitEngine) {
        this.executor = executor;
        this.cache = cache;
        this.counters = counters;
        this.jitEngine = jitEngine;
    }

    /**
     * Gets execution counters used for profiling before compilation decision
     *
     * @return execution counters associated with given scheduling session
     */
    public ExecutionCounters counters() {
        return counters;
    }

    /**
     * Gets thread-safe lock-free global method cache used for store
     * compilation results and broadcasting to executor threads
     *
     * @return global method associated with given scheduling session
     */
    public MethodCache cache() {
        return cache;
    }

    /**
     * Gets execution engine used for submitting compiled methods to execute or not yet compiled to interpreter
     *
     * @return execution engine associated with given scheduling session
     */
    public ExecutionEngine executor() {
        return executor;
    }

    /**
     * Gets jit compilation engine used for scheduling compilation tasks
     * and make number of currently compiling something threads bounded
     *
     * @return jit engine associated with given scheduling session
     */
    public JitEngine jitEngine() {
        return jitEngine;
    }
}
