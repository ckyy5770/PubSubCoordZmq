package edu.vanderbilt.chuilian.loadbalancer.plan;

/**
 * Created by Killian on 6/5/17.
 */

import edu.vanderbilt.chuilian.loadbalancer.BrokerReportAnalyzer;
import edu.vanderbilt.chuilian.loadbalancer.ChannelReport;

/**
 * channel plan generator provide some algorithm to generate new channel plan, all method are static, and this class cannot be instantiate
 */
public class ChannelPlanGenerator {
    private ChannelPlanGenerator() {
    }

    public static ChannelPlan defualt(ChannelReport report, ChannelPlan oldPlan, BrokerReportAnalyzer brokerReportAnalyzer) {
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
            if (oldPlan.getStrategy() == Strategy.ALL_SUB && oldPlan.getNumBrokers() == numBrokers) {
                oldPlan.turnOld(); // a way to tell outside world that this channel plan doesn't change from the old version
                return oldPlan;
            }
            return replicate(Strategy.ALL_SUB, numBrokers, report.getTopic(), brokerReportAnalyzer);
        } else if (p > ALL_SUB_THRESHOLD && numPublications > PUBLICATION_THRESHOLD) {
            int numBrokers = (int) Math.floor(p / ALL_SUB_THRESHOLD);
            if (oldPlan.getStrategy() == Strategy.ALL_SUB && oldPlan.getNumBrokers() == numBrokers) {
                oldPlan.turnOld();
                return oldPlan;
            }
            return replicate(Strategy.ALL_SUB, numBrokers, report.getTopic(), brokerReportAnalyzer);
        } else if (s > ALL_PUB_THRESHOLD && numSubscribers > SUBSCRIBER_THRESHOLD) {
            int numBrokers = (int) Math.floor(s / ALL_PUB_THRESHOLD);
            if (oldPlan.getStrategy() == Strategy.ALL_PUB && oldPlan.getNumBrokers() == numBrokers) {
                oldPlan.turnOld();
                return oldPlan;
            }
            return replicate(Strategy.ALL_PUB, numBrokers, report.getTopic(), brokerReportAnalyzer);
        } else {
            if (oldPlan.getStrategy() == Strategy.HASH) {
                oldPlan.turnOld();
                return oldPlan;
            }
            return replicate(Strategy.HASH, 0, report.getTopic(), brokerReportAnalyzer);
        }
    }

    private static ChannelPlan replicate(Strategy strategy, int numBrokers, String topic, BrokerReportAnalyzer brokerReportAnalyzer) {
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
        }
        return newPlan;
    }

}
