package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/8/17.
 */

import java.util.PriorityQueue;

/**
 * Id Pool initializes with a very large a mount of number, and it will only give the smallest number it has as the next id.
 */
public class IdPool {
    private final PriorityQueue<Integer> priorityQueue;

    public IdPool(int numLimit) {
        priorityQueue = new PriorityQueue<>(numLimit);
        for (int i = 0; i < numLimit; i++) {
            priorityQueue.add(i);
        }
    }

    public int fetchID() {
        return priorityQueue.poll();
    }

    public void returnID(int id) {
        priorityQueue.add(id);
    }
}
