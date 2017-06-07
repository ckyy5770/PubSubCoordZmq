package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/5/17.
 */

import edu.vanderbilt.chuilian.loadbalancer.plan.ChannelPlan;

import java.util.*;

/**
 * this analyzer will take broker report map to initialize then calculate critical statistics needed for channel plan generator
 */
// TODO: 6/5/17 TBD 
public class BrokerReportAnalyzer {
    ArrayList<BrokerReport> brokerReports;
    // merged channel reports combines channel reports of the same topic into one. It only gives basic information needed to perform channel-level rebalancing.
    // it serves like a "channel view" of the original broker reports, and it will not be changed during the decision making process.
    HashMap<String, ChannelReport> mergedChannelReports;

    public BrokerReportAnalyzer(BrokerLoadReportBuffer brokerLoadReportBuffer) {
        this.brokerReports = brokerLoadReportBuffer.toList();
        this.brokerReports.sort((BrokerReport a, BrokerReport b) -> (int) (a.getLoadRatio() - b.getLoadRatio()));
        // constructing merged channel reports
        this.mergedChannelReports = new HashMap<>();
        for (BrokerReport report : brokerReports) {
            for (Map.Entry<String, ChannelReport> entry : report.entrySet()) {
                String topic = entry.getKey();
                ChannelReport channelReport = entry.getValue();
                if (!mergedChannelReports.containsKey(topic)) {
                    // this channel doesnt exist in current merged result, create a new one
                    mergedChannelReports.put(topic, new ChannelReport(topic));
                }
                // merge
                mergedChannelReports.get(topic).mergeWith(channelReport);
            }
        }
    }

    public HashMap<String, ChannelReport> getMergedChannelReports() {
        return mergedChannelReports;
    }

    public Set<String> getLeastBusyBrokers(int numBrokers) {
        if (brokerReports.size() < numBrokers)
            throw new RuntimeException("there are not enough number of brokers to perform channel-level rebalancing.");
        Set<String> res = new HashSet<>();
        for (int i = 0; i < numBrokers; i++) {
            res.add(brokerReports.get(i).getBrokerID());
        }
        return res;
    }

    public BrokerReport getMostBusyBroker() {
        return brokerReports.get(brokerReports.size());
    }

    public BrokerReport getLeastBusyBroker() {
        return brokerReports.get(0);
    }

    public ChannelReport getMostBusyChannel(BrokerReport brokerReport) {
        return brokerReport.getMostBusyChannel();
    }

    public boolean isBrokerSafe(BrokerReport brokerReport, ChannelReport channelReport, double LR_THRESHOLD) {
        double newLR = (brokerReport.getLoadRatio() * brokerReport.getBandWidthBytes() + (double) channelReport.getNumIOBytes()) / brokerReport.getBandWidthBytes();
        return newLR < LR_THRESHOLD;
    }

    // TODO: 6/6/17 please carefully see if this two functions work properly when debugging, since this implementation is a little ugly
    public void applyHighLoadPlan(BrokerReport maxBrokerReportRef, BrokerReport minBrokerReportRef, ChannelReport maxChannelReportRef) {
        minBrokerReportRef.addChannelReport(maxChannelReportRef);
        maxBrokerReportRef.removeChannelReport(maxChannelReportRef.getTopic());
        this.brokerReports.sort((BrokerReport a, BrokerReport b) -> (int) (a.getLoadRatio() - b.getLoadRatio()));
    }

    public void applyChannelPlan(ChannelPlan newChannelPlan, ChannelReport mergedChannelReport) {
        // divide overall load of this channel into n equal pieces
        int numNewBrokers = newChannelPlan.getNumBrokers();
        ArrayList<ChannelReport> reports = mergedChannelReport.divideInto(numNewBrokers);
        // remove all reports about this channel
        removeAllReportsOfOneChannel(mergedChannelReport.getTopic());
        // add new assigned channel (as in the form of fake reports) to the brokers
        addNewReportsToBrokers(reports, newChannelPlan.getAvailableBroker());
        this.brokerReports.sort((BrokerReport a, BrokerReport b) -> (int) (a.getLoadRatio() - b.getLoadRatio()));
    }

    private void removeAllReportsOfOneChannel(String topic) {
        for (int i = 0; i < brokerReports.size(); i++) {
            brokerReports.get(i).removeChannelReport(topic);
        }
    }

    private void addNewReportsToBrokers(ArrayList<ChannelReport> reports, Set<String> brokerIDs) {
        int counter = 0;
        for (int i = 0; i < brokerReports.size(); i++) {
            if (brokerIDs.contains(brokerReports.get(i).getBrokerID())) {
                brokerReports.get(i).addChannelReport(reports.get(counter++));
            }
        }
    }


}
