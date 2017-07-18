package edu.vanderbilt.chuilian.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.ExecutorService;


/**
 * Created by Killian on 7/18/17.
 */
class TestPQ {

    public static void main1(String[] args) throws InterruptedException, ExecutionException {
        int nThreads = 20;
        int qInitialSize = 40;

        ExecutorService exec = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
                new PriorityBlockingQueue<Runnable>(qInitialSize, new PriorityFutureComparator())) {

            protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
                RunnableFuture<T> newTaskFor = super.newTaskFor(runnable, value);
                return new PriorityFuture<T>(newTaskFor, ((PriorityRunnable) runnable).getPriority());
            }
        };

        List<TestJob> tjList = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            int priority = i;
            //System.out.println("Scheduling: " + priority);
            TestJob job = new TestJob(priority);
            tjList.add(job);
            exec.submit(job);
        }
        Thread.sleep(2000);
        exec.shutdownNow();
        for(int i=0; i<20; i++){
            TestJob tj = tjList.get(i);
            System.out.println("counter " + i + ": " + tj.getCounter());
        }
    }


    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int nThreads = 1;
        int qInitialSize = 100;

        ExecutorService execHigher = Executors.newFixedThreadPool(20);

        //ExecutorService exec2 = Executors.newFixedThreadPool(20);

        ExecutorService exec = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
                new PriorityBlockingQueue<Runnable>(qInitialSize, new PriorityFutureComparator())) {

            protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
                RunnableFuture<T> newTaskFor = super.newTaskFor(runnable, value);
                return new PriorityFuture<T>(newTaskFor, ((TestOneTimeJobSub) runnable).getPriority());
            }
        };

        List<TestOneTimeJob> tjList = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            int priority = i;
            //System.out.println("Scheduling: " + priority);
            TestOneTimeJob job = new TestOneTimeJob(priority,exec);
            tjList.add(job);
            execHigher.submit(job);
        }
        Thread.sleep(2000);
        execHigher.shutdownNow();
        Thread.sleep(2000);
        exec.shutdownNow();
        for(int i=0; i<20; i++){
            TestOneTimeJob tj = tjList.get(i);
            System.out.println("counter " + i + ": " + tj.getCounter());
        }
    }
}