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
import java.util.stream.Collectors;

public class SolutionThread extends UserThread {

    private static class CachedMethodID implements MethodID {

        private final MethodID base;

        public CachedMethodID(MethodID base) {
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

    private static <K, V> Map<K, V> mapCopyWithLock(Map<K, V> base, ReadWriteLock readWriteLock) {
        readWriteLock.readLock().lock();
        try {
            return base.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public SolutionThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);

        if (jitEngineInitialized.tryLock() && jitEngine == null) {
            jitEngine = Executors.newFixedThreadPool(compilationThreadBound);
            jitEngine.execute(() -> {
                while (true) {
                    var hotnessCopy = mapCopyWithLock(hotnessGlobal, hotnessGlobalLock);
                    var compiledL1Copy = mapCopyWithLock(compiledL1Global, compiledL1GlobalLock);
                    var compiledL2Copy = mapCopyWithLock(compiledL2Global, compiledL2GlobalLock);

                    var newCompiledL1Keys = new ArrayList<CachedMethodID>();
                    var newCompiledL1Values = new ArrayList<Future<CompiledMethod>>();
                    var newCompiledL2Keys = new ArrayList<CachedMethodID>();
                    var newCompiledL2Values = new ArrayList<Future<CompiledMethod>>();
                    for (var entry : hotnessCopy.entrySet()) {
                        var id = entry.getKey();
                        var hotness = entry.getValue();

                        if (hotness > 10000 && compiledL2Copy.getOrDefault(id, null) == null) {
                            var compiled = jitEngine.submit(() -> compiler.compile_l2(id.base()));
                            newCompiledL2Keys.add(id);
                            newCompiledL2Values.add(compiled);
                        } else if (hotness > 1000 && compiledL1Copy.getOrDefault(id, null) == null) {
                            var compiled = jitEngine.submit(() -> compiler.compile_l1(id.base()));
                            newCompiledL1Keys.add(id);
                            newCompiledL1Values.add(compiled);
                        }
                    }

                    compiledL1GlobalLock.writeLock().lock();
                    try {
                        for (int i = 0; i < newCompiledL1Keys.size(); i++) {
                            compiledL1Global.put(newCompiledL1Keys.get(i), newCompiledL1Values.get(i));
                        }
                    } finally {
                        compiledL1GlobalLock.writeLock().unlock();
                    }

                    compiledL2GlobalLock.writeLock().lock();
                    try {
                        for (int i = 0; i < newCompiledL2Keys.size(); i++) {
                            compiledL2Global.put(newCompiledL2Keys.get(i), newCompiledL2Values.get(i));
                        }
                    } finally {
                        compiledL2GlobalLock.writeLock().unlock();
                    }
                }
            });
        }
    }

    private CompiledMethod lookup(MethodID methodID) {
        var cachedMethodId = new CachedMethodID(methodID);
        var lookupL2 = compiledL2.getOrDefault(cachedMethodId, null);
        if (lookupL2 == null) {
            return compiledL1.getOrDefault(cachedMethodId, null);
        }
        return lookupL2;
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

    private void replicateCaches() {
        var compiledL1Copy = mapCopyWithLock(compiledL1Global, compiledL1GlobalLock);
        var compiledL2Copy = mapCopyWithLock(compiledL2Global, compiledL2GlobalLock);

        for (var entry : compiledL1Copy.entrySet()) {
            var cachedMethodID = entry.getKey();
            var compiledMethodFuture = entry.getValue();

            if (compiledMethodFuture.isDone()) {
                try {
                    compiledL1.put(cachedMethodID, compiledMethodFuture.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        for (var entry : compiledL2Copy.entrySet()) {
            var cachedMethodID = entry.getKey();
            var compiledMethodFuture = entry.getValue();

            if (compiledMethodFuture.isDone()) {
                try {
                    compiledL2.put(cachedMethodID, compiledMethodFuture.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public ExecutionResult executeMethod(MethodID methodID) {
        var compiled = lookup(methodID);

        ExecutionResult result;
        if (compiled != null) {
            result = exec.execute(compiled);
        } else {
            result = exec.interpret(methodID);
        }

        updateHotness(methodID);
        replicateCaches();
        return result;
    }
}