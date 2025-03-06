package org.nsu.syspro.parprog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.nsu.syspro.parprog.base.DefaultFork;
import org.nsu.syspro.parprog.base.DiningTable;
import org.nsu.syspro.parprog.control.Scheduler;
import org.nsu.syspro.parprog.examples.DefaultPhilosopher;
import org.nsu.syspro.parprog.helpers.TestLevels;
import org.nsu.syspro.parprog.interfaces.Fork;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class CustomSchedulingTest extends TestLevels {

    static class CustomizedPhilosopher extends DefaultPhilosopher {
        @Override
        public void onHungry(Fork left, Fork right) {
            sleepMillis(this.id * 20);
            System.out.println(Thread.currentThread() + " " + this + ": onHungry");
            super.onHungry(left, right);
        }
    }

    static final class CustomizedFork extends DefaultFork {
        @Override
        public void acquire() {
            System.out.println(Thread.currentThread() + " trying to acquire " + this);
            super.acquire();
            System.out.println(Thread.currentThread() + " acquired " + this);
            sleepMillis(100);
        }
    }

    static final class CustomizedTable extends DiningTable<CustomizedPhilosopher, CustomizedFork> {
        public CustomizedTable(int N) {
            super(N);
        }

        @Override
        public CustomizedFork createFork() {
            return new CustomizedFork();
        }

        @Override
        public CustomizedPhilosopher createPhilosopher() {
            return new CustomizedPhilosopher();
        }
    }

    @AfterEach
    public void reset() {
        Scheduler.reset();
    }

    @EnabledIf("easyEnabled")
    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4, 5})
    @Timeout(3)
    void testDeadlockFreedom(int N) {
        final CustomizedTable table = dine(new CustomizedTable(N), 1);
    }

    @EnabledIf("easyEnabled")
    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4, 5}) // cases 2 and 3 can fail - while 1 second they have no chance to take fork
    @Timeout(3)
    void testSingleSlow(int N) {
        var table = new DiningTable<DefaultPhilosopher, DefaultFork>(N) {
            @Override
            public DefaultFork createFork() {
                return new DefaultFork();
            }

            @Override
            public DefaultPhilosopher createPhilosopher() {
                return new DefaultPhilosopher();
            }
        };
        table.phils.set(0, new DefaultPhilosopher() {
            @Override
            public void eat(Fork f1, Fork f2) {
                f1.acquire();
                try {
                    f2.acquire();
                    try {
                        sleepSeconds(1);
                        countMeal();
                    } finally {
                        f2.release();
                    }
                } finally {
                    f1.release();
                }
            }
        });

        var max = dine(table, 1).maxMeals();
        if (N > 3) {
            assertTrue(max >= 1000);
        }
    }

    @EnabledIf("mediumEnabled")
    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4, 5})
    @Timeout(2)
    void testWeakFairness(int N) {
        var table = new DiningTable<DefaultPhilosopher, DefaultFork>(N) {
            @Override
            public DefaultFork createFork() {
                return new DefaultFork();
            }

            @Override
            public DefaultPhilosopher createPhilosopher() {
                return new DefaultPhilosopher();
            }
        };
        for (int i = 0; i < table.phils.size(); i++) {
            if (i % 2 == 0) {
                table.phils.set(i, new DefaultPhilosopher() {
                    @Override
                    public void onHungry(Fork left, Fork right) {
                        sleepMillis(10);
                        super.onHungry(left, right);
                    }
                });
            } else {
                table.phils.set(i, new DefaultPhilosopher() {
                    @Override
                    public void onHungry(Fork left, Fork right) {
                        sleepMillis(1);
                        super.onHungry(left, right);
                    }
                });
            }
        }

        dine(table, 1);
        assertTrue(table.minMeals() > 0); // every philosopher eat at least once
    }

    @EnabledIf("hardEnabled")
    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4, 5})
    @Timeout(2)
    void testImpossibleFairness(int N) {
        var table = new DiningTable<DefaultPhilosopher, DefaultFork>(N) {
            @Override
            public DefaultFork createFork() {
                return new DefaultFork();
            }

            @Override
            public DefaultPhilosopher createPhilosopher() {
                return new DefaultPhilosopher();
            }
        };
        var rand = new Random();
        var ateFastMap = new boolean[N];
        for (int i = 0; i < N; i++) {
            ateFastMap[i] = rand.nextBoolean();
        }
        int winnerIdx = rand.nextInt(N); // ensure that here at least one fast eater
        int loserIdx; // and loser
        while ((loserIdx = rand.nextInt(N)) == winnerIdx);
        ateFastMap[winnerIdx] = true;
        ateFastMap[loserIdx] = false;

        for (int i = 0; i < table.phils.size(); i++) {
            if (ateFastMap[i]) {
                table.phils.set(i, new DefaultPhilosopher());
            } else {
                table.phils.set(i, new DefaultPhilosopher() {
                    @Override
                    public void onHungry(Fork left, Fork right) {
                        sleepMillis(1 + rand.nextInt(4));
                        super.onHungry(left, right);
                    }
                });
            }
        }

        dine(table, 1);
        final long minMeals = table.minMeals();
        final long maxMeals = table.maxMeals();
        assertFalse(maxMeals < 1.5 * minMeals); // some king of gini index for philosophers (should fail here)
    }
}
