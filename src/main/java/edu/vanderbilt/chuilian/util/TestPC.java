package edu.vanderbilt.chuilian.util;

import java.util.concurrent.*;

/**
 * Created by Killian on 7/19/17.
 */
public class TestPC {
    PriorityBlockingQueue<Integer> bpq = new PriorityBlockingQueue<>();
    int counter = 100;

    void producer(){
        System.out.println("produce: " + counter);
        bpq.put(counter--);
    }

    void consumer(){
        try {
            int num = bpq.take();
            System.out.println("consume: " + num);
        }catch (Exception e){

        }
    }

    public int getCounter() {
        return counter;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        TestPC pc = new TestPC();

        ExecutorService es = Executors.newFixedThreadPool(2);


        es.submit(()->{
            while(true){
                pc.consumer();
            }
        });

        es.submit(()->{
            while(pc.getCounter() >= 0){
                pc.producer();
            }
        });
    }
}
