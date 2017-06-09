package edu.vanderbilt.chuilian.loadbalancer.plan;

/**
 * Created by Killian on 6/5/17.
 */

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SystemPlan {
    String from;
    String to;
    String channel;

    private static final Logger logger = LogManager.getLogger(SystemPlan.class.getName());

    public SystemPlan(String from, String to, String channel) {
        this.from = from;
        this.to = to;
        this.channel = channel;
    }

    public SystemPlan() {
        this.from = null;
        this.to = null;
        this.channel = null;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }
    public void setFrom(String from) {
        this.from = from;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getChannel() {
        return channel;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }
}