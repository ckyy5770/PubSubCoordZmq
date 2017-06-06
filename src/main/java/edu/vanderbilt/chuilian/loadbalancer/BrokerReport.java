package edu.vanderbilt.chuilian.loadbalancer;

import edu.vanderbilt.chuilian.types.TypesBrokerReport;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Killian on 6/1/17.
 */

/**
 * channel reports for one broker.
 */
public class BrokerReport {
    // topic, channelReport
    HashMap<String, ChannelReport> map;
    String brokerID;
    double loadRatio;
    double bandWidthBytes;

    public BrokerReport(String brokerID) {
        this.map = new HashMap<>();
        this.brokerID = brokerID;
    }

    public BrokerReport(TypesBrokerReport typesBrokerReport) {
        this.brokerID = typesBrokerReport.brokerID();
        this.loadRatio = typesBrokerReport.loadRatio();
        this.bandWidthBytes = typesBrokerReport.bandWidthBytes();
        this.map = new HashMap<>();
        // typesBrokerReport -> brokerReport
        for (int i = 0; i < typesBrokerReport.channelReportsLength(); i++) {
            // TypesChannelReport -> channelReport
            this.addChannelReport(new ChannelReport(typesBrokerReport.channelReports(i)));
        }
    }

    public String getBrokerID() {
        return brokerID;
    }

    public double getBandWidthBytes() {
        return bandWidthBytes;
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

    public void addChannelReport(ChannelReport channelReport) {
        map.put(channelReport.getTopic(), channelReport);
    }

    public void removeChannelReport(ChannelReport channelReport) {
        map.remove(channelReport.getTopic());
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

    // calculations
    public double getLoadRatio() {
        long cumulativeIOBytes = 0;
        for (Map.Entry<String, ChannelReport> entry : map.entrySet()) {
            cumulativeIOBytes += entry.getValue().getNumIOBytes();
        }
        return (double) cumulativeIOBytes / LoadAnalyzer.getBandWidthBytes();
    }

    public ChannelReport getMostBusyChannel() {
        ChannelReport res = null;
        for (Map.Entry<String, ChannelReport> entry : map.entrySet()) {
            if (res == null || entry.getValue().getNumIOBytes() > res.getNumIOBytes()) {
                res = entry.getValue();
            }
        }
        return res;
    }


}

