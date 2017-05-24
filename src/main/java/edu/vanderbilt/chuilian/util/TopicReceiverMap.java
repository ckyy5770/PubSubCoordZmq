package edu.vanderbilt.chuilian.util;

/**
 * Created by Killian on 5/24/17.
 */

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * topic --> corresponding data receiver ref
 */
public class TopicReceiverMap {
    final DefaultReceiver defaultReceiver;
    ConcurrentHashMap<String, DataReceiver> map;

    // this data structure should only be initialized given a default receiver address
    public TopicReceiverMap(DefaultReceiver defaultReceiver) {
        this.defaultReceiver = defaultReceiver;
        this.map = new ConcurrentHashMap<>();
    }

    /**
     * get corresponding data receiver for given topic
     *
     * @param topic
     * @return null if no such data receiver exist
     */
    public DataReceiver get(String topic) {
        return this.map.get(topic);
    }

    /**
     * register a topic with given sender address, this will register the topic to the TopicReceiverMap and return the reference to newly created DataReceiver
     *
     * @param topic
     * @return return newly created receiver, if the receiver for the topic already exist, return null
     */
    public DataReceiver register(String topic, String address, MsgBufferMap msgBufferMap, ExecutorService executor) {
        if (this.map.containsKey(topic)) return null;
        else {
            DataReceiver newReceiver = new DataReceiver(topic, address, msgBufferMap, executor);
            this.map.put(topic, newReceiver);
            return newReceiver;
        }
    }

    /**
     * delete a topic along with the receiver that corresponding to it
     *
     * @param topic
     * @return the previous receiver associated with topic, or null if there was no mapping for topic
     */
    public DataReceiver unregister(String topic) {
        return map.remove(topic);
    }

    /**
     * get default data receiver
     *
     * @return default data receiver should always exist as long as broker and publisher started properly.
     */
    public DefaultReceiver getDefault() {
        return this.defaultReceiver;
    }
}
