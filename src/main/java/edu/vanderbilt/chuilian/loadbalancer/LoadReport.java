package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/6/17.
 */

import java.util.HashMap;

/**
 * processing-friendly version of BrokerLoadReportBuffer
 */
public class LoadReport {
    // brokerID, BrokerReport
    private HashMap<String, BrokerReport> map;

    public LoadReport() {
        this.map = new HashMap<>();
    }

    public void addBrokerReport(BrokerReport brokerReport) {
        map.put(brokerReport.getBrokerID(), brokerReport);
    }

    public int size() {
        return map.size();
    }
}
