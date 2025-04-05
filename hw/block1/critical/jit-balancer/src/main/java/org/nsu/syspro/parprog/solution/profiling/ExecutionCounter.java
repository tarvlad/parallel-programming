package org.nsu.syspro.parprog.solution.profiling;

import org.nsu.syspro.parprog.solution.jit.JitEngine;

/**
 * Counter which represents how many times given method was executed and is this method suitable for L1/L2 compilation
 */
public class ExecutionCounter {
    private long compilations;

    public ExecutionCounter() {
        compilations = 0L;
    }

    /**
     * Increments current counter value
     */
    public void count() {
        if (compilations == Long.MAX_VALUE) {
            return;
        }
        compilations++;
    }

    /**
     * Increments current counter value and return value of counter after increment
     *
     * @return counter value after increment
     */
    public int countAndRead() {
        count();
        return value();
    }

    /**
     * @return associated with given counter number of method execution times
     */
    public int value() {
        return (int)compilations;
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
