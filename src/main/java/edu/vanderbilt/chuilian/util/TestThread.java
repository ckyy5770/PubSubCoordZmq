package edu.vanderbilt.chuilian.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by Killian on 7/18/17.
 */
class TestThread {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        List<TestJob> tjList = new ArrayList<>();
        List<Thread> tList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            int priority = i;
            //System.out.println("Scheduling: " + priority);
            TestJob job = new TestJob(priority);
            tjList.add(job);
            Thread t = new Thread(job);
            t.setPriority(priority +1);
            tList.add(t);
            t.start();
        }
        Thread.sleep(2000);
        for(int i=0; i<3; i++){
            tList.get(i).interrupt();
        }
        for(int i=0; i<3; i++){
            TestJob tj = tjList.get(i);
            System.out.println("counter " + i + ": " + tj.getCounter());
        }
    }
}
