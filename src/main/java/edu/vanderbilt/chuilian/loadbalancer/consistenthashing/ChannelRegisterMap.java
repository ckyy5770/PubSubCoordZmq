package edu.vanderbilt.chuilian.loadbalancer.consistenthashing;

import edu.vanderbilt.chuilian.loadbalancer.IdPool;

import java.util.HashMap;

/**
 * Created by Killian on 6/8/17.
 */

/**
 * similar to BrokerRegisterMap
 */
public class ChannelRegisterMap {
    // TODO: 6/8/17 hard coded number limit
    private final IdPool idPool = new IdPool(10000);
    // topic, ID
    HashMap<String, Integer> map = new HashMap<>();

    public ChannelRegisterMap() {
    }

    /**
     * @param topic
     * @return ID for consistent hashing
     */
    public int register(String topic) {
        int newId = idPool.fetchID();
        map.put(topic, newId);
        return newId;
    }

    public void unregister(String topic) {
        map.remove(topic);
    }

    /**
     * return null if no such topic in brokerIdMapping
     *
     * @param topic
     * @return
     */
    public Integer getID(String topic) {
        return map.get(topic);
    }
}
