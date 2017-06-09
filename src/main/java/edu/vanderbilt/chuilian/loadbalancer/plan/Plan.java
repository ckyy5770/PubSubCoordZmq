package edu.vanderbilt.chuilian.loadbalancer.plan;

import edu.vanderbilt.chuilian.loadbalancer.BrokerReportAnalyzer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

/**
 * Created by Killian on 6/2/17.
 */
public class Plan {
    // version
    long version = 0;
    // executable plan --> channel mapping
    ChannelMapping channelMapping = new ChannelMapping();

    private static final Logger logger = LogManager.getLogger(BrokerReportAnalyzer.class.getName());

    public Plan() {
    }

    public Plan(long version, ChannelMapping channelMapping) {
        this.version = version;
        this.channelMapping = channelMapping;
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
        if (isChanged) {
            version++;
            logger.info("Current Plan Changed. Version: {}", version);
        }
    }

    public long getVersion() {
        return version;
    }

}




