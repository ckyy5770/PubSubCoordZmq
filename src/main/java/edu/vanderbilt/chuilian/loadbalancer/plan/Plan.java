package edu.vanderbilt.chuilian.loadbalancer.plan;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Killian on 6/2/17.
 */
public class Plan {
    HashMap<String, ChannelPlan> channelPlans = null;
    Set<HighLoadPlan> highLoadPlans = null;
    Set<LowLoadPlan> lowLoadPlans = null;

    public Plan() {
    }

    /**
     * modifiers: addChannelPlan, addHighLoadPlan, addLowLoadPlan
     */


    public void addChannelPlan(ChannelPlan channelPlan) {
        if (channelPlans == null) channelPlans = new HashMap<>();
        this.channelPlans.put(channelPlan.getTopic(), channelPlan);
    }

    public void addHighLoadPlan(HighLoadPlan highLoadPlan) {
        if (highLoadPlans == null) highLoadPlans = new HashSet<>();
        this.highLoadPlans.add(highLoadPlan);
    }

    public void addLowLoadPlan(LowLoadPlan lowLoadPlan) {
        if (lowLoadPlans == null) lowLoadPlans = new HashSet<>();
        this.lowLoadPlans.add(lowLoadPlan);
    }

    /**
     * lookup methods: getChannelPlan, getHighLoadPlans, getLowLoadPlans, getOneChannelPlan
     */

    public HashMap<String, ChannelPlan> getChannelPlans() {
        return channelPlans;
    }

    public Set<HighLoadPlan> getHighLoadPlans() {
        return highLoadPlans;
    }

    public Set<LowLoadPlan> getLowLoadPlans() {
        return lowLoadPlans;
    }

    public ChannelPlan getOneChannelPlan(String topic) {
        return channelPlans.get(topic);
    }

}




