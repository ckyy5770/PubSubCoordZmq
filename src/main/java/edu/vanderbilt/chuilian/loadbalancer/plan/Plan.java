package edu.vanderbilt.chuilian.loadbalancer.plan;

import java.util.ArrayList;

/**
 * Created by Killian on 6/2/17.
 */
public class Plan {
    // version
    long version = 0;
    // executable plan --> channel mapping
    ChannelMapping channelMapping = new ChannelMapping();

    public Plan() {
    }

    public ChannelMapping getChannelMapping() {
        return channelMapping;
    }

    public void applyNewPlan(ArrayList<ChannelPlan> channelPlans, ArrayList<HighLoadPlan> highLoadPlans, ArrayList<LowLoadPlan> lowLoadPlans) {
        boolean isChanged = false;
        if (channelPlans != null && channelPlans.size() != 0) {
            isChanged = true;
            channelMapping.replaceChannelPlans(channelPlans);
        }
        if (highLoadPlans != null && highLoadPlans.size() != 0) {
            isChanged = true;
            channelMapping.migrateChannels(highLoadPlans);
        }
        if (lowLoadPlans != null && lowLoadPlans.size() != 0) {
            isChanged = true;
            channelMapping.migrateChannels(lowLoadPlans);
        }
        if (isChanged) version++;
    }

}




