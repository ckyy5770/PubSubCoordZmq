package edu.vanderbilt.chuilian.loadbalancer.consistenthashing;

/**
 * Created by Killian on 6/8/17.
 */

import edu.vanderbilt.chuilian.loadbalancer.IdPool;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * this brokerIdMapping register a ID for a new coming broker, and unregister it if it's down
 */
public class BrokerRegisterMap {
    // TODO: 6/8/17 hard coded number limit
    private final IdPool idPool = new IdPool(1000);
    // brokerID, ID -> one to one mapping
    private final HashMap<String, Integer> brokerIdMapping = new HashMap<>();
    private final HashMap<Integer, String> idBrokerMapping = new HashMap<>();
    // for fast searching for nearest key
    private final TreeMap<Integer, String> idBrokerTreeMap = new TreeMap<>();

    // max size of this map
    private int maxSize = 0;

    public BrokerRegisterMap() {
    }

    /**
     * @param brokerID
     * @return ID for consistent hashing
     */
    public int register(String brokerID) {
        int newId = idPool.fetchID();
        brokerIdMapping.put(brokerID, newId);
        idBrokerMapping.put(newId, brokerID);
        idBrokerTreeMap.put(newId, brokerID);
        maxSize = Math.max(brokerIdMapping.size(), maxSize);
        return newId;
    }

    public void unregister(String brokerID) {
        Integer id = getID(brokerID);
        brokerIdMapping.remove(brokerID);
        idBrokerMapping.remove(brokerID);
        idBrokerTreeMap.remove(brokerID);
        idPool.returnID(id);
    }

    /**
     * return null if no such brokerID in brokerIdMapping
     *
     * @param brokerID
     * @return
     */
    public Integer getID(String brokerID) {
        return brokerIdMapping.get(brokerID);
    }

    public String getBroker(Integer id) {
        return idBrokerMapping.get(id);
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int size() {
        return brokerIdMapping.size();
    }

    public Map.Entry<Integer, String> getUpperEntry(int id) {
        return idBrokerTreeMap.higherEntry(id);
    }

    public Map.Entry<Integer, String> getLowerEntry(int id) {
        return idBrokerTreeMap.lowerEntry(id);
    }

}
