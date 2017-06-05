package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/1/17.
 */
// TODO: 6/5/17 need change.
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