package edu.vanderbilt.chuilian.loadbalancer.plan;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Killian on 6/5/17.
 */

public class ChannelPlan {
    public enum Strategy {
        HASH, ALL_SUB, ALL_PUB
    }

    private String topic;
    private Set<String> availableBroker;
    private Strategy strategy;

    public ChannelPlan() {
        this.topic = null;
        this.strategy = null;
        this.availableBroker = null;
    }

    public ChannelPlan(String topic, Set<String> availableBroker, Strategy strategy) {
        this.topic = topic;
        this.availableBroker = availableBroker;
        this.strategy = strategy;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void addAvailableBroker(String brokerID) {
        if (availableBroker == null) availableBroker = new HashSet<String>();
        availableBroker.add(brokerID);
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    public String getTopic() {
        return this.topic;
    }

    public Strategy getStrategy() {
        return this.strategy;
    }

    public Set<String> getAvailableBroker() {
        return this.availableBroker;
    }
}