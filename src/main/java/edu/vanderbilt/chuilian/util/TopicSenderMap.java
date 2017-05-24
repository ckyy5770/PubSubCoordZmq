package edu.vanderbilt.chuilian.util;

/**
 * Created by Killian on 5/24/17.
 */

import java.util.concurrent.ConcurrentHashMap;

/**
 * topic --> corresponding data sender ref
 */
public class TopicSenderMap {
    final DefaultSender defaultSender;
    ConcurrentHashMap<String, DataSender> map;

    // this data structure should only be initialized given a default receiver address
    public TopicSenderMap(DefaultSender defaultSender) {
        this.defaultSender = defaultSender;
        this.map = new ConcurrentHashMap<>();
    }

    /**
     * get corresponding data sender for given topic
     *
     * @param topic
     * @return null if no such data sender exist
     */
    public DataSender get(String topic) {
        return this.map.get(topic);
    }

    /**
     * register a topic with given receiver address, this will register the topic to the TopicSenderMap and return the reference to newly created DataSender
     *
     * @param topic
     * @return return newly created sender, if the sender for the topic already exist, return null
     */
    public DataSender register(String topic, String address) {
        if (this.map.contains(topic)) return null;
        else {
            DataSender newSender = new DataSender(topic, address);
            this.map.put(topic, newSender);
            return newSender;
        }
    }

    /**
     * delete a topic along with the sender that corresponding to it
     *
     * @param topic
     * @return the previous sender associated with topic, or null if there was no mapping for topic
     */
    public DataSender unregister(String topic) {
        return map.remove(topic);
    }

    /**
     * get default data sender
     *
     * @return default data sender should always exist as long as broker and publisher started properly.
     */
    public DefaultSender getDefault() {
        return this.defaultSender;
    }
}
