package edu.vanderbilt.chuilian.clients.subscriber;

/**
 * Created by Killian on 5/24/17.
 */

import edu.vanderbilt.chuilian.util.MsgBufferMap;
import edu.vanderbilt.chuilian.util.ZkConnect;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * topic --> corresponding data receiverFromLB ref
 */
public class TopicReceiverMap {
    final DefaultReceiver defaultReceiver;
    ConcurrentHashMap<String, DataReceiver> map;

    // this data structure should only be initialized given a default receiverFromLB address
    public TopicReceiverMap(DefaultReceiver defaultReceiver) {
        this.defaultReceiver = defaultReceiver;
        this.map = new ConcurrentHashMap<>();
    }

    /**
     * get corresponding data receiverFromLB for given topic
     *
     * @param topic
     * @return null if no such data receiverFromLB exist
     */
    public DataReceiver get(String topic) {
        return map.get(topic);
    }

    /**
     * register a topic with given sender address, this will register the topic to the TopicReceiverMap and return the reference to newly created DataReceiver
     *
     * @param topic
     * @return return newly created receiverFromLB, if the receiverFromLB for the topic already exist, return null
     */
    public DataReceiver register(String topic, String address, MsgBufferMap msgBufferMap, ExecutorService executor, ZkConnect zkConnect) {
        if (map.containsKey(topic)) return null;
        else {
            DataReceiver newReceiver = new DataReceiver(topic, address, msgBufferMap, executor, zkConnect);
            map.put(topic, newReceiver);
            return newReceiver;
        }
    }

    /**
     * delete a topic along with the receiverFromLB that corresponding to it, before delete the receiverFromLB, stop it first.
     *
     * @param topic
     * @return the previous receiverFromLB associated with topic, or null if there was no mapping for topic
     */
    public DataReceiver unregister(String topic) {
        return map.remove(topic);
    }

    /**
     * get default data receiverFromLB
     *
     * @return default data receiverFromLB should always exist as long as broker and publisher started properly.
     */
    public DefaultReceiver getDefault() {
        return this.defaultReceiver;
    }

    /**
     * Returns a Set view of the mappings contained in this map. for iterating the map
     *
     * @return
     */
    public Set<Map.Entry<String, DataReceiver>> entrySet() {
        return map.entrySet();
    }
}
