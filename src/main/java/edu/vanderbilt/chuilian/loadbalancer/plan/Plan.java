package edu.vanderbilt.chuilian.loadbalancer.plan;

import edu.vanderbilt.chuilian.loadbalancer.BrokerReportAnalyzer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    public void updateVersion() {
        version++;
    }

    /**
     * this method will translate current plan to a broker-specific view, it will tell the specific broker, which channel is belong to it.
     *
     * @param brokerID
     * @return a set of topic name
     */
    public Set<String> getBrokerView(String brokerID) {
        Set<String> res = new HashSet<>();
        for (Map.Entry<String, ChannelPlan> entry : channelMapping.entrySet()) {
            if (entry.getValue().getAvailableBroker().contains(brokerID)) res.add(entry.getKey());
        }
        return res;
    }

}




