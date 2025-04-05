package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;
import org.nsu.syspro.parprog.solution.caching.LocalMethodCache;
import org.nsu.syspro.parprog.solution.caching.MethodCacheEntry;
import org.nsu.syspro.parprog.solution.profiling.ExecutionCounter;

import static org.nsu.syspro.parprog.solution.caching.MethodCacheEntry.Kind.L1;
import static org.nsu.syspro.parprog.solution.caching.MethodCacheEntry.Kind.L2;

/**
 * Wrapper for UserThread which controls execution of methods according to schedule policy from task requirements
 */
public class SolutionThread extends UserThread {

    private final SessionContext globalContext;
    private final LocalMethodCache localCache = new LocalMethodCache();

    public SolutionThread(SessionContext context, Runnable r) {
        super(r);
        this.globalContext = context;
    }

    private void processCompileDecision(MethodID methodID, MethodCacheEntry localEntry) {
        if (localEntry.kind() != L2) {
            globalContext.reverseArcLock().lock();
            try {
                MethodCacheEntry globalEntry = globalContext.cache().lookup(methodID);
                if (globalEntry.kind() != MethodCacheEntry.Kind.EMPTY) {
                    localCache.put(methodID, globalEntry);
                }

                if (globalEntry.kind() != L2) {
                    int counter = globalContext.counters().counter(methodID).countAndRead();

                    if (ExecutionCounter.l2Ready(counter)) {
                        globalContext.cache().putIfAbsent(
                                methodID, L2, globalContext.jitEngine().l2Submitter(methodID)
                        );
                        localCache.put(methodID, MethodCacheEntry.forL1Entry(globalContext.cache().ensureCompiled(methodID, L2)));
                    } else if (ExecutionCounter.l1Ready(counter)) {
                        globalContext.cache().putIfAbsent(
                                methodID, L1, globalContext.jitEngine().l1Submitter(methodID)
                        );
                        localCache.put(methodID, MethodCacheEntry.forL1Entry(globalContext.cache().ensureCompiled(methodID, L1)));
                    }
                }
            } finally {
                globalContext.reverseArcLock().unlock();
            }
        }
    }

    /**
     * Executes method with given method id according to scheduling rules.
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

        processCompileDecision(methodID, localEntry);
        return result;
    }
}