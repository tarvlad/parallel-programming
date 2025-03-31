package org.nsu.syspro.parprog.solution.jit;

import org.nsu.syspro.parprog.external.CompilationEngine;
import org.nsu.syspro.parprog.external.CompiledMethod;
import org.nsu.syspro.parprog.external.MethodID;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * Utility class for control max number of parallel executed L1 / L2 compilation tasks
 */
public class JitEngine {
    public static final int L1_COMPILE_DECISION = 1000;
    public static final int L2_COMPILE_DECISION = 10000;

    private final ExecutorService jitEngine;
    private final CompilationEngine compiler;

    public JitEngine(ExecutorService jitEngine, CompilationEngine compiler) {
        this.jitEngine = jitEngine;
        this.compiler = compiler;
    }

    /**
     * Returns supplier which, when called, will perform submit new L1 compilation task
     *
     * @param id method id to determine which method will be L1 compiled
     * @return supplier with pending compilation
     */
    public Supplier<Future<CompiledMethod>> l1Submitter(MethodID id) {
        return () -> jitEngine.submit(() -> compiler.compile_l1(id));
    }

    /**
     * Returns supplier which, when called, will perform submit new L2 compilation task
     *
     * @param id method id to determine which method will be L2 compiled
     * @return supplier with pending compilation
     */
    public Supplier<Future<CompiledMethod>> l2Submitter(MethodID id) {
        return () -> jitEngine.submit(() -> compiler.compile_l2(id));
    }
}
