package edu.vanderbilt.chuilian.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Killian on 7/18/17.
 */
class TestOneTimeJobSub implements PriorityRunnable {
    private int priority;
    private AtomicInteger counter;

    public TestOneTimeJobSub(int priority, AtomicInteger counter) {
        this.priority = priority;
        this.counter = counter;
    }

    public void run() {
        //System.out.println("Starting: " + priority);
        try {
            System.out.println("Executing: " + priority);
            Thread.sleep(100000);
        }catch(Exception e){

        }
        int newval = this.counter.get() + 1;
        this.counter.set(newval);
        /*
        long num = 1000000;
        for (int i = 0; i < 1000000; i++) {
            num *= Math.random() * 1000;
            num /= Math.random() * 1000;
            if (num == 0)
                num = 1000000;
        }
        */
    }

    public int getPriority() {
        return priority;
    }

}

