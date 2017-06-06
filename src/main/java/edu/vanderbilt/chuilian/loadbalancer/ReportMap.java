package edu.vanderbilt.chuilian.loadbalancer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Killian on 6/1/17.
 */

public class ReportMap {
    HashMap<String, ChannelReport> map;
    String brokerID;


    public ReportMap(String brokerID) {
        this.map = new HashMap<>();
        this.brokerID = brokerID;
    }

    public String getBrokerID() {
        return brokerID;
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

    public void updatePublications(String topic, long publications) {
        if (!map.containsKey(topic)) {
            map.put(topic, new ChannelReport(topic));
        }
        map.get(topic).numPublications += publications;
    }

    public void updateSubscribers(String topic, long subscribers) {
        if (!map.containsKey(topic)) {
            map.put(topic, new ChannelReport(topic));
        }
        map.get(topic).numSubscribers += subscribers;
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

    public double getLoadRatio() {
        long cumulativeIOBytes = 0;
        for (Map.Entry<String, ChannelReport> entry : map.entrySet()) {
            cumulativeIOBytes += entry.getValue().getNumIOBytes();
        }
        return (double) cumulativeIOBytes / LoadAnalyzer.getBandWidthBytes();
    }

}

