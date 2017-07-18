package edu.vanderbilt.chuilian.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Killian on 7/18/17.
 */
class TestOneTimeJob implements PriorityRunnable {
    private int priority;
    private ExecutorService es;
    private AtomicInteger counter = new AtomicInteger(0);

    public TestOneTimeJob(int priority, ExecutorService es) {
        this.priority = priority;
        this.es = es;
    }

    public void run() {
        //System.out.println("Starting: " + priority);
        try {
            //System.out.println("priority: " + priority + " threadPrior: " + Thread.currentThread().getPriority());
            //System.out.println("Executing: " + priority);
            while(true){
                TestOneTimeJobSub job = new TestOneTimeJobSub(priority, counter);
                System.out.println("TaskSubmitting: " + priority);
                es.submit(job);

                Thread.sleep(50);
            }
        }catch(Exception e){

        }
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

    public int getCounter() {
        return counter.get();
    }
}

