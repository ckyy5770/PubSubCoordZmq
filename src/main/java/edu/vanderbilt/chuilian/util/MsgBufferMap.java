package edu.vanderbilt.chuilian.util;

/**
 * Created by Killian on 5/24/17.
 */

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * data receiver/sender will store messages into the buffer of a specific topic
 */
public class MsgBufferMap {
    private ConcurrentHashMap<String, MsgBuffer> map;

    public MsgBufferMap() {
        this.map = new ConcurrentHashMap<>();
    }

    /**
     * register a topic, create a new message buffer for it
     *
     * @param topic
     * @return new message buffer, null if the topic already exist
     */
    public MsgBuffer register(String topic) {
        if (map.containsKey(topic)) return null;
        else {
            MsgBuffer newBuffer = new MsgBuffer(topic);
            map.put(topic, newBuffer);
            return newBuffer;
        }
    }

    /**
     * delete a topic from the map along with the buffer that corresponding to it
     *
     * @param topic
     * @return the previous buffer ref associated with topic, or null if there was no mapping for topic
     */
    public MsgBuffer unregister(String topic) {
        return map.remove(topic);
    }

    /**
     * Returns a Set view of the mappings contained in this map. for iterating the map
     *
     * @return
     */
    public Set<Map.Entry<String, MsgBuffer>> entrySet() {
        return map.entrySet();
    }

    /**
     * get the buffer
     *
     * @param topic
     * @return
     */
    public MsgBuffer get(String topic) {
        return map.get(topic);
    }
}
