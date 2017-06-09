package edu.vanderbilt.chuilian.loadbalancer.plan;

/**
 * Created by Killian on 6/5/17.
 */

import edu.vanderbilt.chuilian.loadbalancer.BrokerReportAnalyzer;
import edu.vanderbilt.chuilian.loadbalancer.ChannelReport;
import edu.vanderbilt.chuilian.loadbalancer.consistenthashing.ConsistentHashingMap;

import java.util.*;

/**
 * channel plan generator provide some algorithm to generate new channel plan, all method are static, and this class cannot be instantiate
 */
public class ChannelPlanGenerator {
    private ChannelPlanGenerator() {
    }

    public static ArrayList<ChannelPlan> generatePlans(Plan oldPlan, BrokerReportAnalyzer brokerReportAnalyzer, ConsistentHashingMap consistentHashingMap) {
        ArrayList<ChannelPlan> res = new ArrayList<>();
        HashMap<String, ChannelReport> mergedChannelReports = brokerReportAnalyzer.getMergedChannelReports();
        for (Map.Entry<String, ChannelReport> entry : mergedChannelReports.entrySet()) {
            String topic = entry.getKey();
            ChannelReport mergedChannelReport = entry.getValue();
            ChannelPlan oldChannelPlan = oldPlan.getChannelMapping().getChannelPlan(topic);
            ChannelPlan newChannelPlan = generateOnePlan(mergedChannelReport, oldChannelPlan, brokerReportAnalyzer, consistentHashingMap);
            if (newChannelPlan != oldChannelPlan) {
                res.add(newChannelPlan);
                brokerReportAnalyzer.applyChannelPlan(newChannelPlan, mergedChannelReport);
            }
        }
        return res;
    }

    private static ChannelPlan generateOnePlan(ChannelReport report, ChannelPlan oldChannelPlan, BrokerReportAnalyzer brokerReportAnalyzer, ConsistentHashingMap consistentHashingMap) {
        double ALL_SUB_THRESHOLD = 0;
        double ALL_PUB_THRESHOLD = 0;
        long PUBLICATION_THRESHOLD = 10;
        long SUBSCRIBER_THRESHOLD = 2;

        long numPublications = report.getNumPublications();
        long numSubscribers = report.getNumSubscribers();
        double p = (double) numPublications / (double) numSubscribers;
        double s = (double) numSubscribers / (double) numPublications;
        if (numPublications > PUBLICATION_THRESHOLD && numSubscribers > SUBSCRIBER_THRESHOLD) {
            // corner case: both publication and subscriber number are large, use ALL_SUB
            int numBrokers = (int) Math.floor(p / ALL_SUB_THRESHOLD);
            if (oldChannelPlan.getStrategy() == Strategy.ALL_SUB && oldChannelPlan.getNumBrokers() == numBrokers)
                return oldChannelPlan;
            return replicate(Strategy.ALL_SUB, numBrokers, report.getTopic(), brokerReportAnalyzer, consistentHashingMap);
        } else if (p > ALL_SUB_THRESHOLD && numPublications > PUBLICATION_THRESHOLD) {
            int numBrokers = (int) Math.floor(p / ALL_SUB_THRESHOLD);
            if (oldChannelPlan.getStrategy() == Strategy.ALL_SUB && oldChannelPlan.getNumBrokers() == numBrokers)
                return oldChannelPlan;
            return replicate(Strategy.ALL_SUB, numBrokers, report.getTopic(), brokerReportAnalyzer, consistentHashingMap);
        } else if (s > ALL_PUB_THRESHOLD && numSubscribers > SUBSCRIBER_THRESHOLD) {
            int numBrokers = (int) Math.floor(s / ALL_PUB_THRESHOLD);
            if (oldChannelPlan.getStrategy() == Strategy.ALL_PUB && oldChannelPlan.getNumBrokers() == numBrokers)
                return oldChannelPlan;
            return replicate(Strategy.ALL_PUB, numBrokers, report.getTopic(), brokerReportAnalyzer, consistentHashingMap);
        } else {
            if (oldChannelPlan.getStrategy() == Strategy.HASH) return oldChannelPlan;
            return replicate(Strategy.HASH, 0, report.getTopic(), brokerReportAnalyzer, consistentHashingMap);
        }
    }

    private static ChannelPlan replicate(Strategy strategy, int numBrokers, String topic, BrokerReportAnalyzer brokerReportAnalyzer, ConsistentHashingMap consistentHashingMap) {
        ChannelPlan newPlan = new ChannelPlan();
        newPlan.setTopic(topic);
        switch (strategy) {
            case ALL_SUB:
                newPlan.setStrategy(Strategy.ALL_SUB);
                newPlan.setAvailableBroker(brokerReportAnalyzer.getLeastBusyBrokers(numBrokers));
            case ALL_PUB:
                newPlan.setStrategy(Strategy.ALL_PUB);
                newPlan.setAvailableBroker(brokerReportAnalyzer.getLeastBusyBrokers(numBrokers));
            case HASH:
                newPlan.setStrategy(Strategy.HASH);
                newPlan.setAvailableBroker(consistentHashing(consistentHashingMap, topic));
        }
        return newPlan;
    }

    // TODO: 6/7/17 need to add consistent hashing logic
    public static Set<String> consistentHashing(ConsistentHashingMap consistentHashingMap, String topic) {
        Set<String> res = new HashSet<String>();
        res.add(consistentHashingMap.getBroker(topic));
        return res;
    }

}
