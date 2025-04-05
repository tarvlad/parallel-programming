package org.nsu.syspro.parprog.helpers;

import com.sun.jdi.Method;
import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.CompilationEngine;
import org.nsu.syspro.parprog.external.ExecutionEngine;
import org.nsu.syspro.parprog.solution.SessionContext;
import org.nsu.syspro.parprog.solution.SolutionThread;
import org.nsu.syspro.parprog.solution.caching.MethodCache;
import org.nsu.syspro.parprog.solution.jit.JitEngine;
import org.nsu.syspro.parprog.solution.profiling.ExecutionCounters;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class TestLevels {

    public abstract TestEnvironment testEnvironment();

    public static int compilationThreadBound() {
        // must be >= 2
        return 3;
    }

    public static UserThread createUserThread(MethodCache methodCache, ExecutionCounters counters, JitEngine jitEngine, ExecutionEngine e, CompilationEngine c, Lock reverseArcLock, Runnable r) {
        SessionContext context = new SessionContext(e, methodCache, counters, jitEngine, reverseArcLock);
        return new SolutionThread(context, r);
    }

    enum Level {
        EASY, MEDIUM, HARD
    }

    private static final Level CURRENT_LEVEL = Level.HARD;

    public static boolean easyEnabled() {
        return CURRENT_LEVEL.ordinal() >= Level.EASY.ordinal();
    }

    public static boolean mediumEnabled() {
        return CURRENT_LEVEL.ordinal() >= Level.MEDIUM.ordinal();
    }

    public static boolean hardEnabled() {
        return CURRENT_LEVEL.ordinal() >= Level.HARD.ordinal();
    }

    public static void sleepNanos(long nanos) {
        if (nanos == 0) {
             return;
        }

        final long millis = Duration.ofNanos(nanos).toMillis();
        final int leftoverNanos = (int) (nanos - Duration.ofMillis(millis).toNanos());
        try {
            Thread.sleep(millis, leftoverNanos);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void sleepSeconds(int seconds) {
        sleepNanos(Duration.ofSeconds(seconds).toNanos());
    }
}
