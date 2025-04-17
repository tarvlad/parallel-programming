package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;
import org.nsu.syspro.parprog.solution.caching.LocalMethodCache;
import org.nsu.syspro.parprog.solution.caching.MethodCacheEntry;
import org.nsu.syspro.parprog.solution.jit.JitEngine;
import org.nsu.syspro.parprog.solution.profiling.LocalExecutionCache;

/**
 * Wrapper for UserThread which controls execution of methods according to schedule policy from task requirements
 */
public class SolutionThread extends UserThread {

    private final SessionContext globalContext;
    private final LocalMethodCache localCache = new LocalMethodCache();
    private final LocalExecutionCache localExecLogBuffer = new LocalExecutionCache(JitEngine.LOCAL_EXEC_BUFFER_SIZE);

    public SolutionThread(SessionContext context, Runnable r) {
        super(r);
        this.globalContext = context;
    }

    /**
     * Executes method with given method id according to scheduling rules.
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

        JitEngine.processCompileDecision(globalContext, localCache, localExecLogBuffer, methodID, localEntry);
        return result;
    }
}