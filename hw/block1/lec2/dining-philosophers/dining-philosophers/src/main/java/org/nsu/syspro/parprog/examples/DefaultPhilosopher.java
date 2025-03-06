package org.nsu.syspro.parprog.examples;

import org.nsu.syspro.parprog.control.Scheduler;
import org.nsu.syspro.parprog.interfaces.Fork;
import org.nsu.syspro.parprog.interfaces.Philosopher;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultPhilosopher implements Philosopher {

    private static final AtomicLong idProvider = new AtomicLong(0);
    public final long id;
    private long successfulMeals;
    private final Lock lock = new ReentrantLock();

    public DefaultPhilosopher() {
        this.id = idProvider.getAndAdd(1);
        this.successfulMeals = 0;
        Scheduler.register(this);
    }

    @Override
    public long meals() {
        return successfulMeals;
    }

    @Override
    public void countMeal() {
        successfulMeals++;
    }

    public void onHungry(Fork left, Fork right) {
        try {
            lock.lock();

            var first = Scheduler.preEnterLock(left);
            var second = Scheduler.preEnterLock(right);
            if (first.tryLock()) {
                try {
                    if (second.tryLock()) {
                        try {
                            eat(left, right);
                        } finally {
                            second.unlock();
                        }
                    }
                } finally {
                    first.unlock();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Lock getLock() {
        return lock;
    }

    @Override
    public String toString() {
        return "DefaultPhilosopher{" +
                "id=" + id +
                '}';
    }
}
