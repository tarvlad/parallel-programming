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
    private static final Lock preEnterLocksConsistency = new ReentrantLock(true);

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
                preEnterLocks.put(f, new ReentrantLock(true));
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
        long value = Long.MAX_VALUE;
        int idx = 0;
        for (int i = 0; i < ate.size(); i++) {
            var iAte = ate.get(i);
            if (iAte < value) {
                idx = i;
                value = iAte;
            }
        }

        return idx;
    }

    private static int incZn(int x, int N) {
        assert x < N && x >= 0;
        if (x == N - 1) {
            return 0;
        }
        return x + 1;
    }

    private static int decZn(int x, int N) {
        assert x < N && x >= 0;
        if (x == 0) {
            return N - 1;
        }
        return x - 1;
    }

    private static void scheduleCycle() {
        if (queue.isEmpty()) {
            Thread.yield();
            return;
        }

        int pausedIdx = maxAteIdx();
        int N = queue.size();
        var pausedLeft = queue.get(decZn(pausedIdx, N));
        var pausedRight = N < 4 ? pausedLeft : queue.get(incZn(pausedIdx, N));
        var pausedLeftLock = pausedLeft.getLock();
        var pausedRightLock = pausedRight.getLock();
        pausedLeftLock.lock();
        try {
            pausedRightLock.lock();
            try {
                Thread.sleep(0, 100000);
                Thread.yield();
            } catch (InterruptedException ignored) {
                Thread.yield();
            } finally {
                pausedRightLock.unlock();
            }
        } finally {
            pausedLeftLock.unlock();
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
