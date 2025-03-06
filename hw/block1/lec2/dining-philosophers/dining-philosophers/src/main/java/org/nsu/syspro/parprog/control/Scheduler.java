package org.nsu.syspro.parprog.control;

import org.nsu.syspro.parprog.interfaces.Fork;
import org.nsu.syspro.parprog.interfaces.Philosopher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Scheduler {

    private static final Lock service = new ReentrantLock();
    private static ArrayList<Philosopher> queue = new ArrayList<>();
    private static ArrayList<Long> ate = new ArrayList<>();
    private static HashMap<Fork, Lock> preEnterLocks = new HashMap<>();
    private static final Lock preEnterLocksConsistency = new ReentrantLock();

    static {
        new Thread(Scheduler::schedule).start();
    }

    public static void reset() {
        try {
            service.lock();
            queue = new ArrayList<>();
            ate = new ArrayList<>();
            preEnterLocks = new HashMap<>();
        } finally {
            service.unlock();
        }
    }

    public static void register(Fork f) {
        try {
            service.lock();
            try {
                preEnterLocksConsistency.lock();
                preEnterLocks.put(f, new ReentrantLock());
            } finally {
                preEnterLocksConsistency.unlock();
            }
        } finally {
            service.unlock();
        }
    }

    public static void register(Philosopher p) {
        try {
            service.lock();
            try {
                preEnterLocksConsistency.lock();
                queue.add(p);
                ate.add(0L);
            } finally {
                preEnterLocksConsistency.unlock();
            }
        } finally {
            service.unlock();
        }
    }

    public static void schedule() {
        while (true) {
            try {
                service.lock();
                scheduleCycle();

                for (int j = 0; j < queue.size(); j++) {
                    ate.set(j, queue.get(j).meals());
                }
            } finally {
                service.unlock();
            }
        }
    }

    private static int maxAteIdx() {
        assert ate.size() == queue.size();
        long value = 0;
        int idx = 0;
        for (int i = 0; i < ate.size(); i++) {
            var iAte = ate.get(i);
            if (iAte > value) {
                idx = i;
                value = iAte;
            }
        }

        return idx;
    }

    private static void scheduleCycle() {
        if (queue.isEmpty()) {
            Thread.yield();
            return;
        }

        int pausedIdx = maxAteIdx();
        var paused = queue.get(pausedIdx);
        var pausedLock = paused.getLock();
        try {
            pausedLock.lock();
            Thread.sleep(0, 100000);
            Thread.yield();
        } catch (InterruptedException ignored) {
            Thread.yield();
        } finally {
            pausedLock.unlock();
        }
    }

    public static Lock preEnterLock(Fork f) {
        try {
            preEnterLocksConsistency.lock();
            return preEnterLocks.get(f);
        } finally {
            preEnterLocksConsistency.unlock();
        }
    }
}
