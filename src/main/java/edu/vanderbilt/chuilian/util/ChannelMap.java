package edu.vanderbilt.chuilian.util;

/**
 * Created by Killian on 5/23/17.
 */

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Channel Map is a hash map that takes the topic name as the key and corresponding msg channel reference as value.
 * each edge broker should have ONE and ONLY ONE channel map.
 * Note: this hash map may be accessed by multiple threads(main channel may add something to it, other channels may delete
 * themselves from it when their life ends), simply make it concurrent might not be enough for efficiency
 * concerns. May need to change concurrency logic for future experiment.
 */
public class ChannelMap {
    private ConcurrentHashMap<String, MsgChannel> map;
    private MainChannel mainChannel;


    public ChannelMap(){
        this.mainChannel = null;
        this.map = new ConcurrentHashMap<>();
    }

    public void setMain(MainChannel mainChannel) {
        this.mainChannel = mainChannel;
    }

    /**
     * Register a topic, this will register the topic to the ChannelMap and return the reference to newly created channel
     * @param topic
     * @return null if the topic already existed
     */
    public MsgChannel register(String topic, PortList portList, ExecutorService executor, ZkConnect zkConnect, ChannelMap channelMap) {
        if(this.map.containsKey(topic)) return null;
        else{
            MsgChannel newChannel = new MsgChannel(topic, portList, executor, zkConnect, channelMap);
            this.map.put(topic, newChannel);
            return newChannel;
        }
    }

    /**
     * delete a topic from the ChannelMap along with the Channel that corresponding to it
     * @param topic
     * @return the previous Channel ref associated with topic, or null if there was no mapping for topic
     */
    public MsgChannel unregister(String topic){
        return this.map.remove(topic);
    }

    /**
     * get channel reference by topic
     * @param topic
     * @return null if topic not existed
     */
    public MsgChannel get(String topic){
        return this.map.get(topic);
    }

    /**
     * get main channel reference
     *
     * @return null if topic not existed
     */
    public MsgChannel getMain() {
        return this.mainChannel;
    }

    /**
     * Returns a Set view of the mappings contained in this map. for iterating the map
     *
     * @return
     */
    public Set<Map.Entry<String, MsgChannel>> entrySet() {
        return this.map.entrySet();
    }

}
