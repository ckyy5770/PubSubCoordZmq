package edu.vanderbilt.chuilian.loadbalancer.plan;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Killian on 6/2/17.
 */
public class Plan {
    HashMap<String, ChannelPlan> channelPlans = null;
    ArrayList<HighLoadPlan> highLoadPlans = null;
    ArrayList<LowLoadPlan> lowLoadPlans = null;

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
        if (highLoadPlans == null) highLoadPlans = new ArrayList<>();
        this.highLoadPlans.add(highLoadPlan);
    }

    public void addLowLoadPlan(LowLoadPlan lowLoadPlan) {
        if (lowLoadPlans == null) lowLoadPlans = new ArrayList<>();
        this.lowLoadPlans.add(lowLoadPlan);
    }

    /**
     * lookup methods: getChannelPlan, getHighLoadPlans, getLowLoadPlans, getOneChannelPlan
     */

    public HashMap<String, ChannelPlan> getChannelPlans() {
        return channelPlans;
    }

    public ArrayList<HighLoadPlan> getHighLoadPlans() {
        return highLoadPlans;
    }

    public ArrayList<LowLoadPlan> getLowLoadPlans() {
        return lowLoadPlans;
    }

    public ChannelPlan getOneChannelPlan(String topic) {
        return channelPlans.get(topic);
    }

}




