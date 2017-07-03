package edu.vanderbilt.chuilian.loadbalancer;

import com.sun.corba.se.pept.broker.Broker;
import edu.vanderbilt.chuilian.types.TypesBrokerReport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Killian on 6/1/17.
 */

/**
 * channel reports for one broker.
 */
public class BrokerReport {
    // topic, channelReport
    ConcurrentHashMap<String, ChannelReport> map;
    String brokerID;
    double loadRatio;
    double bandWidthBytes;

    private static final Logger logger = LogManager.getLogger(BrokerReport.class.getName());

    public BrokerReport(String brokerID) {
        this.map = new ConcurrentHashMap<>();
        this.brokerID = brokerID;
    }

    public BrokerReport(TypesBrokerReport typesBrokerReport) {
        this.brokerID = typesBrokerReport.brokerID();
        this.loadRatio = typesBrokerReport.loadRatio();
        this.bandWidthBytes = typesBrokerReport.bandWidthBytes();
        this.map = new ConcurrentHashMap<>();
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

    private void setLoadRatio(double loadRatio) {
        this.loadRatio = loadRatio;
    }

    // bytes can be positive and negative
    private void changeLoadRatioByBytes(double bytes) {
        this.setLoadRatio((loadRatio * bandWidthBytes + bytes) / bandWidthBytes);
    }

    public void updateBytes(String topic, long bytes) {
        if (!map.containsKey(topic)) {
            Long[] i1000 = new Long[1000];
            for(int i = 0; i< 1000; i++){
                i1000[i] = new Long(1);
            }
            String a = new String(topic);
            ChannelReport newChannelReport =  new ChannelReport(topic);
            map.put(topic, newChannelReport);
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
        this.map = new ConcurrentHashMap<>();
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

    public void addChannelReport(ChannelReport channelReport) {
        map.put(channelReport.getTopic(), channelReport);
        changeLoadRatioByBytes(-channelReport.getNumIOBytes());
    }

    public void removeChannelReport(String topic) {
        if (!map.containsKey(topic)) return;
        changeLoadRatioByBytes(-(map.get(topic).getNumIOBytes()));
        map.remove(topic);
    }


}

