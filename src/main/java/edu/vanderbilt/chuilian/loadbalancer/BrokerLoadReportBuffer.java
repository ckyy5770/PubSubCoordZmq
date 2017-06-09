package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/2/17.
 */

import edu.vanderbilt.chuilian.types.TypesBrokerReport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * a collection of load reports, receiving from all brokers. Keep the most recent broker reports.
 * only one thread will change the content of this map.
 */
public class BrokerLoadReportBuffer {
    // brokerID, latest report
    private final HashMap<String, TypesBrokerReport> map;

    public BrokerLoadReportBuffer() {
        this.map = new HashMap<>();
    }

    public BrokerLoadReportBuffer(BrokerLoadReportBuffer that) {
        this.map = new HashMap<>(that.map);
    }

    public boolean exist(String brokerID) {
        return map.get(brokerID) != null;
    }

    public void update(String brokerID, TypesBrokerReport report) {
        // if the report already exists, the new one will replace the old one.
        map.put(brokerID, report);
    }

    public BrokerLoadReportBuffer snapShot() {
        return new BrokerLoadReportBuffer(this);
    }

    public ArrayList<BrokerReport> toList() {
        ArrayList<BrokerReport> res = new ArrayList<>();
        for (Map.Entry<String, TypesBrokerReport> entry : map.entrySet()) {
            res.add(new BrokerReport(entry.getValue()));
        }
        return res;
    }

    public LoadReport toLoadReport() {
        LoadReport newReport = new LoadReport();
        for (Map.Entry<String, TypesBrokerReport> entry : map.entrySet()) {
            newReport.addBrokerReport(new BrokerReport(entry.getValue()));
        }
        return newReport;
    }



}
