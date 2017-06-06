package edu.vanderbilt.chuilian.loadbalancer;

import edu.vanderbilt.chuilian.types.TypesChannelReport;

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

    ChannelReport(String topic) {
        this.topic = topic;
    }

    ChannelReport(TypesChannelReport typesChannelReport) {
        this.topic = typesChannelReport.topic();
        this.numIOBytes = typesChannelReport.numIOBytes();
        this.numIOMsgs = typesChannelReport.numIOMsgs();
        this.numSubscribers = typesChannelReport.numSubscribers();
        this.numPublications = typesChannelReport.numPublications();
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
}