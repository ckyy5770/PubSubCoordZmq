package edu.vanderbilt.chuilian.util;

import java.util.concurrent.*;

/**
 * Created by Killian on 7/18/17.
 */
class TestJob implements PriorityRunnable {
    private int priority;
    private int counter = 0;

    public TestJob(int priority) {
        this.priority = priority;
    }

    public void run() {
        //System.out.println("Starting: " + priority);
        try {

            if(priority == 0){
                Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
            }else if(priority == 2) {
                Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            }else{
                Thread.currentThread().setPriority(Thread.NORM_PRIORITY);
            }

            System.out.println("priority: " + priority + " threadPrior: " + Thread.currentThread().getPriority());

            while (true) {
                //System.out.println("Executing: " + priority);
                counter++;
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

    public int getCounter(){return counter;}
}

