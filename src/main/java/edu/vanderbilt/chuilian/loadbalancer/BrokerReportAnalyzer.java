package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/5/17.
 */

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * this analyzer will take broker report map to initialize then calculate critical statistics needed for channel plan generator
 */
// TODO: 6/5/17 TBD 
public class BrokerReportAnalyzer {
    ArrayList<BrokerReport> reports;

    public BrokerReportAnalyzer(BrokerLoadReportBuffer brokerLoadReportBuffer) {
        this.reports = brokerLoadReportBuffer.toList();
        this.reports.sort((BrokerReport a, BrokerReport b) -> (int) (a.getLoadRatio() - b.getLoadRatio()));
    }

    public Set<String> getLeastBusyBrokers(int numBrokers) {
        if (reports.size() < numBrokers)
            throw new RuntimeException("there are not enough number of brokers to perform channel-level rebalancing.");
        Set<String> res = new HashSet<>();
        for (int i = 0; i < numBrokers; i++) {
            res.add(reports.get(i).getBrokerID());
        }
        return res;
    }

    public BrokerReport getMostBusyBroker() {
        return reports.get(reports.size());
    }

    public BrokerReport getLeastBusyBroker() {
        return reports.get(0);
    }

    public ChannelReport getMostBusyChannel(BrokerReport brokerReport) {
        return brokerReport.getMostBusyChannel();
    }

    public boolean isBrokerSafe(BrokerReport brokerReport, ChannelReport channelReport, double LR_THRESHOLD) {
        double newLR = (brokerReport.getLoadRatio() * brokerReport.getBandWidthBytes() + (double) channelReport.getNumIOBytes()) / brokerReport.getBandWidthBytes();
        return newLR < LR_THRESHOLD;
    }

    // TODO: 6/6/17 please carefully see if this function work properly when debugging, since this implementation is a little ugly
    public void applyHighLoadPlan(BrokerReport maxBrokerReportRef, BrokerReport minBrokerReportRef, ChannelReport maxChannelReportRef) {
        minBrokerReportRef.addChannelReport(maxChannelReportRef);
        maxBrokerReportRef.removeChannelReport(maxChannelReportRef);
    }


}
