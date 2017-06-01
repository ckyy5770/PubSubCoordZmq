package edu.vanderbilt.chuilian.loadbalancer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Killian on 6/1/17.
 */

public class ReportMap {
    HashMap<String, ChannelReport> map;

    public ReportMap() {
        this.map = new HashMap<>();
    }

    public void updateBytes(String topic, long bytes) {
        if (!map.containsKey(topic)) {
            map.put(topic, new ChannelReport(topic));
        }
        map.get(topic).numIOBytes += bytes;
    }

    public void updateMsgs(String topic, long msgs) {
        if (!map.containsKey(topic)) {
            map.put(topic, new ChannelReport(topic));
        }
        map.get(topic).numIOMsgs += msgs;
    }

    public Set<Map.Entry<String, ChannelReport>> entrySet() {
        return map.entrySet();
    }

    public int size() {
        return map.size();
    }

    public void reset() {
        this.map = new HashMap<>();
    }
}

