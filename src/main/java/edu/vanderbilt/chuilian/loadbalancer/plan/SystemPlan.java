package edu.vanderbilt.chuilian.loadbalancer.plan;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Killian on 6/5/17.
 */
public class SystemPlan {
    String from;
    String to;
    Set<String> channels;

    public SystemPlan(String from, String to) {
        this.from = from;
        this.to = to;
    }

    public SystemPlan() {
        this.from = null;
        this.to = null;
        this.channels = null;
    }

    public void addChannel(String channel) {
        if (channels == null) channels = new HashSet<String>();
        this.channels.add(channel);
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public Set<String> getChannels() {
        return channels;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }
}