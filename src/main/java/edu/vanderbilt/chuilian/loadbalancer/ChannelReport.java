package edu.vanderbilt.chuilian.loadbalancer;

import edu.vanderbilt.chuilian.types.TypesChannelReport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

/**
 * Created by Killian on 6/1/17.
 */

public class ChannelReport {
    String topic;
    // metrics
    long numIOBytes = 0;
    long numIOMsgs = 0;
    long numPublications = 0;
    long numSubscribers = 0;

    private static final Logger logger = LogManager.getLogger(ChannelReport.class.getName());

    public ChannelReport(String topic) {
        this.topic = topic;
    }

    public ChannelReport(TypesChannelReport typesChannelReport) {
        this.topic = typesChannelReport.topic();
        this.numIOBytes = typesChannelReport.numIOBytes();
        this.numIOMsgs = typesChannelReport.numIOMsgs();
        this.numSubscribers = typesChannelReport.numSubscribers();
        this.numPublications = typesChannelReport.numPublications();
    }

    public ChannelReport(ChannelReport originalReport, int shrinkingParameter) {
        this.topic = originalReport.topic;
        this.numIOBytes = originalReport.getNumIOBytes() / shrinkingParameter;
        this.numIOMsgs = originalReport.getNumIOMsgs() / shrinkingParameter;
        this.numPublications = originalReport.getNumPublications() / shrinkingParameter;
        this.numSubscribers = originalReport.getNumSubscribers() / shrinkingParameter;
    }

    public long getNumIOBytes() {
        return numIOBytes;
    }

    public long getNumIOMsgs() {
        return numIOMsgs;
    }

    public String getTopic() {
        return topic;
    }

    public long getNumPublications() {
        return numPublications;
    }

    public long getNumSubscribers() {
        return numSubscribers;
    }

    public void mergeWith(ChannelReport that) {
        if (that.getTopic() != this.getTopic())
            throw new IllegalArgumentException("cant merge two channel reports with different topics!");
        this.numIOBytes += that.numIOBytes;
        this.numIOMsgs += that.numIOMsgs;
        this.numPublications += that.numPublications;
        this.numSubscribers += that.numSubscribers;
    }

    public ArrayList<ChannelReport> divideInto(int n) {
        ArrayList<ChannelReport> res = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            res.add(new ChannelReport(this, n));
        }
        return res;
    }
}