package edu.vanderbilt.chuilian.loadbalancer.plan;

/**
 * Created by Killian on 6/6/17.
 */


import edu.vanderbilt.chuilian.loadbalancer.BrokerReport;
import edu.vanderbilt.chuilian.loadbalancer.BrokerReportAnalyzer;
import edu.vanderbilt.chuilian.loadbalancer.ChannelReport;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

/**
 * system plan generator provide some algorithm to generate new system plan, all method are static, and this class cannot be instantiate
 * NOTE: system plan generator should run before channel plan generator, and they should share the same broker report analyzer.
 */
public class SystemPlanGenerator {
    private static final Logger logger = LogManager.getLogger(SystemPlanGenerator.class.getName());
    private SystemPlanGenerator() {
    }

    public static ArrayList<HighLoadPlan> highLoadPlanGenerator(BrokerReportAnalyzer brokerReportAnalyzer) {
        double LR_THRESHOLD = 0.95;
        ArrayList<HighLoadPlan> plans = new ArrayList<>();
        while (true) {
            BrokerReport maxReport = brokerReportAnalyzer.getMostBusyBroker();
            if (maxReport == null) return plans;
            //logger.info("max broker:{} max lr:{} max bw:{}", maxReport.getBrokerID(), maxReport.getLoadRatio(), maxReport.getBandWidthBytes());
            double maxLR = maxReport.getLoadRatio();
            if (maxLR < LR_THRESHOLD) return plans;
            String maxBrokerID = maxReport.getBrokerID();
            ChannelReport maxChannel = brokerReportAnalyzer.getMostBusyChannel(maxReport);
            BrokerReport minReport = brokerReportAnalyzer.getLeastBusyBroker();
            String minBrokerID = minReport.getBrokerID();
            // re-evaluate to see if the plan is safe, i.e. if the minChannel is able to handle the busiest channel in the busiest broker.
            if (!brokerReportAnalyzer.isBrokerSafe(minReport, maxChannel, LR_THRESHOLD))
                throw new RuntimeException("System-level rebalancing failed: Need more brokers.");
            HighLoadPlan newPlan = new HighLoadPlan(maxBrokerID, minBrokerID, maxChannel.getTopic());
            plans.add(newPlan);
            // here we change the internal state of broker report analyzer, the new plan only applied to the analyzer, it will then affect the decision making process of Channel Plan Generator (see NOTE above).
            brokerReportAnalyzer.applyHighLoadPlan(maxReport, minReport, maxChannel);
        }
    }

    public static ArrayList<LowLoadPlan> lowLoadPlanGenerator(BrokerReportAnalyzer brokerReportAnalyzer) {
        return null;
    }

}
