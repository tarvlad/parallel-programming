package org.nsu.syspro.parprog.solution.profiling;

import org.nsu.syspro.parprog.solution.jit.JitEngine;

import java.util.concurrent.CountDownLatch;

/**
 * Counter which represents how many times given method was executed and is this method suitable for L1/L2 compilation.
 * Provides interface for thread-safe lock-free (*) incrementing its own value.
 * <p>
 * Capable to store up to {@link JitEngine.L2_COMPILE_DECISION} values,
 * next increments will do nothing to value but provide consistency same as read of volatile integer
 * <p>
 * (*) Due to thread-safe lock-free implementation of {@link CountDownLatch.Sync.tryReleaseShared} with CAS
 */
public class ExecutionCounter {
    private final CountDownLatch compilations;

    public ExecutionCounter() {
        compilations = new CountDownLatch(JitEngine.L2_COMPILE_DECISION);
    }

    /**
     * Thread-safe lock-free increments given counter by one
     */
    public void count() {
        compilations.countDown();
    }

    private int normalized() {
        return JitEngine.L2_COMPILE_DECISION - (int)compilations.getCount();
    }

    /**
     * Gets current value holding by counter which is inside interval [0; JitEngine.L2_COMPILE_DECISION]
     * @return associated with given counter number of method execution times
     */
    public int value() {
        return normalized();
    }

    /**
     * Tells possibility of L1 compilation if some method executed given amount of times
     *
     * @param execTimes how many times some method was executed
     * @return {@code true} if given number of compilation times make such method capable to L1 compilation
     */
    public static boolean l1Ready(int execTimes) {
        return execTimes >= JitEngine.L1_COMPILE_DECISION;
    }

    /**
     * Tells possibility of L2 compilation if some method executed given amount of times
     *
     * @param execTimes how many times some method was executed
     * @return {@code true} if given number of compilation times make such method capable to L2 compilation
     */
    public static boolean l2Ready(int execTimes) {
        return execTimes >= JitEngine.L2_COMPILE_DECISION;
    }
}
