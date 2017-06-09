package edu.vanderbilt.chuilian.loadbalancer.consistenthashing;

import java.util.Map;

/**
 * Created by Killian on 6/8/17.
 */
public class ConsistentHashingMap {
    private final BrokerRegisterMap brokerRegisterMap;
    private final ChannelRegisterMap channelRegisterMap;

    public ConsistentHashingMap() {
        brokerRegisterMap = new BrokerRegisterMap();
        channelRegisterMap = new ChannelRegisterMap();
    }

    public void registerBroker(String brokerID) {
        brokerRegisterMap.register(brokerID);
    }

    public void registerChannel(String topic) {
        channelRegisterMap.register(topic);
    }

    public void unregisterBroker(String brokerID) {
        brokerRegisterMap.unregister(brokerID);
    }

    public void unregisterChannel(String topic) {
        channelRegisterMap.unregister(topic);
    }

    /**
     * @param topic
     * @return null if topic not registered
     */
    public String getBroker(String topic) {
        Integer channelID = channelRegisterMap.getID(topic);
        if (channelID == null) return null;
        return consistentHashing(channelID);
    }

    /**
     * core algorithm of consistent hashing
     *
     * @param id
     * @return
     */
    private String consistentHashing(int id) {
        int numSlot = brokerRegisterMap.getMaxSize();
        int hash = id % numSlot;
        // try to find a broker with exact hashed ID
        String res = brokerRegisterMap.getBroker(hash);
        if (res != null) return res;
        // here we need to find the nearest available broker
        Map.Entry<Integer, String> upper = brokerRegisterMap.getUpperEntry(hash);
        Map.Entry<Integer, String> lower = brokerRegisterMap.getLowerEntry(hash);
        // if one of them doesn't exist, return the other
        if (upper == null) return lower.getValue();
        if (lower == null) return upper.getValue();
        // if both of them exists, return the nearest one
        int upperdif = upper.getKey() - hash;
        int lowerdif = hash - lower.getKey();
        // if they are same, randomly choose one, note: must choose randomly
        if (upperdif == lowerdif) return Math.random() < .5 ? upper.getValue() : lower.getValue();
        // if they are not same, return the nearest one
        return upperdif - lowerdif < 0 ? upper.getValue() : lower.getValue();
    }
}
