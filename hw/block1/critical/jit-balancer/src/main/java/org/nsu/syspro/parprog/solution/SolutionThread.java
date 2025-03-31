package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;
import org.nsu.syspro.parprog.solution.caching.LocalMethodCache;
import org.nsu.syspro.parprog.solution.caching.MethodCacheEntry;
import org.nsu.syspro.parprog.solution.profiling.ExecutionCounter;

/**
 * Wrapper for UserThread, which control execution of methods according to schedule policy from task requirements
 */
public class SolutionThread extends UserThread {

    private final SessionContext globalContext;
    private final LocalMethodCache localCache = new LocalMethodCache();

    public SolutionThread(SessionContext context, Runnable r) {
        super(r);
        this.globalContext = context;
    }

    /**
     * Executes method given method id according to scheduling rules.
     * Performs method cache lookup, updates hotness counters
     * and sends compilation schedule requests according to hotness of given method
     *
     * @param methodID id of method, execution of which need to perform
     * @return result, returned by executed method
     */
    @Override
    public ExecutionResult executeMethod(MethodID methodID) {
        ExecutionResult result;
        MethodCacheEntry localEntry = localCache.lookup(methodID);

        if (localEntry.kind() == MethodCacheEntry.Kind.EMPTY) {
            result = globalContext.executor().interpret(methodID);
        } else {
            result = globalContext.executor().execute(localEntry.method());
        }

        if (localEntry.kind() != MethodCacheEntry.Kind.L2) {
            MethodCacheEntry globalEntry = globalContext.cache().lookup(methodID);
            if (globalEntry.kind() != MethodCacheEntry.Kind.EMPTY) {
                localCache.put(methodID, globalEntry);
            }

            if (globalEntry.kind() != MethodCacheEntry.Kind.L2) {
                ExecutionCounter counter = globalContext.counters().counter(methodID);
                counter.count();
                int counterValue = counter.value();

                if (ExecutionCounter.l2Ready(counterValue)) {
                    globalContext.cache().putIfAbsent(
                            methodID, MethodCacheEntry.Kind.L2, globalContext.jitEngine().l2Submitter(methodID)
                    );
                } else if (ExecutionCounter.l1Ready(counterValue)) {
                    globalContext.cache().putIfAbsent(
                            methodID, MethodCacheEntry.Kind.L1, globalContext.jitEngine().l1Submitter(methodID)
                    );
                }
            }
        }

        return result;
    }
}