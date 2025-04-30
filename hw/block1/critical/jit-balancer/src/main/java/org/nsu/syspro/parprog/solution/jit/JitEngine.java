package org.nsu.syspro.parprog.solution.jit;

import org.nsu.syspro.parprog.external.CompilationEngine;
import org.nsu.syspro.parprog.external.CompiledMethod;
import org.nsu.syspro.parprog.external.MethodID;
import org.nsu.syspro.parprog.solution.SessionContext;
import org.nsu.syspro.parprog.solution.caching.LocalMethodCache;
import org.nsu.syspro.parprog.solution.caching.MethodCacheEntry;
import org.nsu.syspro.parprog.solution.profiling.ExecutionCounter;
import org.nsu.syspro.parprog.solution.profiling.ExecutionCounters;
import org.nsu.syspro.parprog.solution.profiling.LocalExecutionCache;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.nsu.syspro.parprog.solution.caching.MethodCacheEntry.Kind.L1;
import static org.nsu.syspro.parprog.solution.caching.MethodCacheEntry.Kind.L2;

/**
 * Utility class for control max number of parallel executed L1 / L2 compilation tasks
 */
public class JitEngine {
    public static final int L1_COMPILE_DECISION = 1000;
    public static final int L2_COMPILE_DECISION = 10000;
    public static final int LOCAL_EXEC_BUFFER_SIZE = 5;

    private final ExecutorService jitEngine;
    private final CompilationEngine compiler;

    public JitEngine(ExecutorService jitEngine, CompilationEngine compiler) {
        this.jitEngine = jitEngine;
        this.compiler = compiler;
    }

    private Supplier<Future<CompiledMethod>> l1Submitter(MethodID id) {
        return () -> jitEngine.submit(() -> compiler.compile_l1(id));
    }

    private Supplier<Future<CompiledMethod>> l2Submitter(MethodID id) {
        return () -> jitEngine.submit(() -> compiler.compile_l2(id));
    }

    /**
     * Performs method cache lookup, updates hotness counters
     * and sends compilation schedule requests according to hotness of given method
     *
     * @param globalContext context of current scheduling session, contains caches and counters
     * @param localCache current user thread local cache
     * @param localExecCache current user thread methods execution counters and log buffer
     * @param methodID id of method to process
     * @param localEntry entry from local method cache related to given methodID
     */
    public static void processCompileDecision(
            SessionContext globalContext,
            LocalMethodCache localCache, LocalExecutionCache localExecCache,
            MethodID methodID, MethodCacheEntry localEntry
    ) {
        if (localEntry.kind() == L2) {
            return;
        }

        int counter = localExecCache.countAndRead(methodID);
        if (!localExecCache.bufferFull()) {
            return;
        }

        ExecutionCounters counters = globalContext.counters();
        AtomicReference<MethodCacheEntry> globalEntry = new AtomicReference<>();
        counters.withLockedCounters(() -> {
            counters.syncWithLocalBuffer(localExecCache.takeBuffer());
            localExecCache.syncWithGlobalCounters(counters);

            globalEntry.set(globalContext.cache().lookup(methodID));
            if (globalEntry.get().kind() != MethodCacheEntry.Kind.EMPTY) {
                localCache.put(methodID, globalEntry.get());
            }
        });

        if (globalEntry.get().kind() != L2) {
            if (ExecutionCounter.l2Ready(counter)) {
                globalContext.cache().cacheLockedAction(() -> {
                    globalContext.cache().putIfAbsent(
                            methodID, L2, globalContext.jitEngine().l2Submitter(methodID)
                    );
                    localCache.put(
                            methodID,
                            MethodCacheEntry.forL1Entry(globalContext.cache().ensureCompiled(methodID, L2))
                    );
                });
            } else if (ExecutionCounter.l1Ready(counter)) {
                globalContext.cache().cacheLockedAction(() -> {
                    globalContext.cache().putIfAbsent(
                            methodID, L1, globalContext.jitEngine().l1Submitter(methodID)
                    );
                    localCache.put(
                            methodID,
                            MethodCacheEntry.forL1Entry(globalContext.cache().ensureCompiled(methodID, L1))
                    );
                });
            }
        }
    }
}
