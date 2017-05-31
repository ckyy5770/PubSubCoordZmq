package edu.vanderbilt.chuilian.clients.subscriber;

import edu.vanderbilt.chuilian.util.MsgBufferMap;
import edu.vanderbilt.chuilian.util.ZkConnect;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Created by Killian on 5/31/17.
 */
public class TopicWaiterMap {
    ConcurrentHashMap<String, Waiter> map;

    public TopicWaiterMap() {
        this.map = new ConcurrentHashMap<>();
    }

    public Waiter get(String topic) {
        return map.get(topic);
    }

    public Waiter register(String topic, MsgBufferMap msgBufferMap, ExecutorService waiterExecutor, ExecutorService receiverExecutor, TopicReceiverMap topicReceiverMap, ZkConnect zkConnect) {
        if (map.containsKey(topic)) return null;
        else {
            Waiter newWaiter = new Waiter(topic, msgBufferMap, waiterExecutor, receiverExecutor, this, topicReceiverMap, zkConnect);
            map.put(topic, newWaiter);
            return newWaiter;

        }
    }

    public Waiter unregister(String topic) {
        return map.remove(topic);
    }

    public Set<Map.Entry<String, Waiter>> entrySet() {
        return map.entrySet();
    }
}
