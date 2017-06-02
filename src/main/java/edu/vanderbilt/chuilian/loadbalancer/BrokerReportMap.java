package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/2/17.
 */

import edu.vanderbilt.chuilian.types.LoadReport;

import java.util.HashMap;

/**
 * only one thread will change the content of this map.
 */
public class BrokerReportMap {
    // brokerID, latest report
    private final HashMap<String, LoadReport> map = new HashMap<>();

    public BrokerReportMap() {
    }

    public void update(String brokerID, LoadReport report) {
        // if the report already exists, the new one will replace the old one.
        map.put(brokerID, report);
    }

    public HashMap<String, LoadReport> snapShot() {
        return new HashMap<>(map);
    }
}
