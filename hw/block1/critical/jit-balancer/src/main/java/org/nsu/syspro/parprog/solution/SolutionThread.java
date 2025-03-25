package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SolutionThread extends UserThread {

    private static class CachedMethodID implements MethodID {

        private final MethodID base;

        public CachedMethodID(MethodID base) {
            assert !(base instanceof CachedMethodID);
            this.base = base;
        }

        public MethodID base() {
            return base;
        }

        @Override
        public long id() {
            return base.id();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof MethodID)) {
                return false;
            }
            var that = (MethodID) o;
            return that.id() == id();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(base);
        }
    }

    private static class MethodCacheLookupResult {

        public enum Kind {
            EMPTY,
            L1,
            L2
        }

        private final CompiledMethod method;
        private final Kind kind;

        public static MethodCacheLookupResult forEmpty() {
            return new MethodCacheLookupResult(null, Kind.EMPTY);
        }

        public static MethodCacheLookupResult forL1Entry(CompiledMethod method) {
            return new MethodCacheLookupResult(method, Kind.L1);
        }

        public static MethodCacheLookupResult forL2Entry(CompiledMethod method) {
            return new MethodCacheLookupResult(method, Kind.L2);
        }

        private MethodCacheLookupResult(CompiledMethod method, Kind kind) {
            this.method = method;
            this.kind = kind;
        }

        public CompiledMethod result() {
            return method;
        }

        public Kind kind() {
            return kind;
        }

        public boolean isEmpty() {
            return kind == Kind.EMPTY;
        }

        public boolean isL1() {
            return kind == Kind.L1;
        }

        public boolean isL2() {
            return kind == Kind.L2;
        }
    }

    private static final Map<CachedMethodID, Long> hotnessGlobal = new HashMap<>();
    private static final ReadWriteLock hotnessGlobalLock = new ReentrantReadWriteLock(true);
    private static final Map<CachedMethodID, Future<CompiledMethod>> compiledL1Global = new HashMap<>();
    private static final ReadWriteLock compiledL1GlobalLock = new ReentrantReadWriteLock(true);
    private static final Map<CachedMethodID, Future<CompiledMethod>> compiledL2Global = new HashMap<>();
    private static final ReadWriteLock compiledL2GlobalLock = new ReentrantReadWriteLock(true);
    private static ExecutorService jitEngine;
    private static final Lock jitEngineInitialized = new ReentrantLock(false);

    private final Map<CachedMethodID, CompiledMethod> compiledL1 = new HashMap<>();
    private final Map<CachedMethodID, CompiledMethod> compiledL2 = new HashMap<>();

    public SolutionThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);

        if (jitEngineInitialized.tryLock() && jitEngine == null) {
            jitEngine = Executors.newFixedThreadPool(compilationThreadBound);
        }
    }

    private MethodCacheLookupResult lookupLocal(MethodID methodID) {
        var cachedMethodId = new CachedMethodID(methodID);
        var lookupL2 = compiledL2.getOrDefault(cachedMethodId, null);
        if (lookupL2 == null) {
            var lookupL1 = compiledL1.getOrDefault(cachedMethodId, null);
            if (lookupL1 == null) {
                return MethodCacheLookupResult.forEmpty();
            }
            return MethodCacheLookupResult.forL1Entry(lookupL1);
        }
        return MethodCacheLookupResult.forL2Entry(lookupL2);
    }

    private static CompiledMethod lookup(MethodID methodID, Map<CachedMethodID, Future<CompiledMethod>> cache, ReadWriteLock cacheReadWriteLock) {
        var cachedMethodId = new CachedMethodID(methodID);

        CompiledMethod lookupResult = null;
        cacheReadWriteLock.readLock().lock();
        try {
            var lookupFuture = cache.getOrDefault(cachedMethodId, null);
            if (lookupFuture != null && lookupFuture.isDone()) {
                lookupResult = lookupFuture.get();
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            cacheReadWriteLock.readLock().unlock();
        }

        return lookupResult;
    }

    private MethodCacheLookupResult lookupGlobalL1(MethodID methodID) {
        var lookup = lookup(methodID, compiledL1Global, compiledL1GlobalLock);
        if (lookup == null) {
            return MethodCacheLookupResult.forEmpty();
        }
        return MethodCacheLookupResult.forL1Entry(lookup);
    }

    private MethodCacheLookupResult lookupGlobalL2(MethodID methodID) {
        var lookup = lookup(methodID, compiledL2Global, compiledL2GlobalLock);
        if (lookup == null) {
            return MethodCacheLookupResult.forEmpty();
        }
        return MethodCacheLookupResult.forL2Entry(lookup);
    }

    private long receiveHotness(MethodID methodID) {
        hotnessGlobalLock.readLock().lock();
        try {
            return hotnessGlobal.getOrDefault(new CachedMethodID(methodID), 0L);
        } finally {
            hotnessGlobalLock.readLock().unlock();
        }
    }

    private void updateHotness(MethodID methodID) {
        hotnessGlobalLock.writeLock().lock();
        try {
            var cachedMethodId = new CachedMethodID(methodID);
            var hotnessValue = hotnessGlobal.getOrDefault(cachedMethodId, 0L);
            hotnessGlobal.put(cachedMethodId, hotnessValue + 1);
        } finally {
            hotnessGlobalLock.writeLock().unlock();
        }
    }

    private void scheduleL2Compilation(MethodID methodID) {
        compiledL2GlobalLock.writeLock().lock();
        try {
            if (compiledL2Global.getOrDefault(new CachedMethodID(methodID), null) != null) {
                return;
            }
            var compiled = jitEngine.submit(() -> compiler.compile_l2(methodID));
            compiledL2Global.put(new CachedMethodID(methodID), compiled);
        } finally {
            compiledL2GlobalLock.writeLock().unlock();
        }
    }

    private void scheduleL1Compilation(MethodID methodID) {
        compiledL1GlobalLock.writeLock().lock();
        try {
            if (compiledL1Global.getOrDefault(new CachedMethodID(methodID), null) != null) {
                return;
            }
            var compiled = jitEngine.submit(() -> compiler.compile_l1(methodID));
            compiledL1Global.put(new CachedMethodID(methodID), compiled);
        } finally {
            compiledL1GlobalLock.writeLock().unlock();
        }
    }

    private void syncWithGlobalCache(MethodID methodID, MethodCacheLookupResult localLookupResult) {
        if (localLookupResult.isL2()) {
            return;
        }

        var lookupL2 = lookupGlobalL2(methodID);
        if (!lookupL2.isEmpty()) {
            compiledL2.put(new CachedMethodID(methodID), lookupL2.result());
            return;
        }

        var hotness = receiveHotness(methodID);
        if (hotness > 10_000) {
            scheduleL2Compilation(methodID);
        }

        if (localLookupResult.isL1()) {
            return;
        }

        var lookupL1 = lookupGlobalL1(methodID);
        if (!lookupL1.isEmpty()) {
            compiledL1.put(new CachedMethodID(methodID), lookupL1.result());
            return;
        }

        if (hotness > 1_000) {
            scheduleL1Compilation(methodID);
        }
    }

    @Override
    public ExecutionResult executeMethod(MethodID methodID) {
        var compiled = lookupLocal(methodID);

        ExecutionResult result;
        if (!compiled.isEmpty()) {
            result = exec.execute(compiled.result());
        } else {
            result = exec.interpret(methodID);
        }

        updateHotness(methodID);
        syncWithGlobalCache(methodID, compiled);
        return result;
    }
}