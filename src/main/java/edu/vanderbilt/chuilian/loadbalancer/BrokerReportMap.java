package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/2/17.
 */

import edu.vanderbilt.chuilian.types.LoadReport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * only one thread will change the content of this map.
 */
public class BrokerReportMap {
    // brokerID, latest report
    private final HashMap<String, LoadReport> map;

    public BrokerReportMap() {
        this.map = new HashMap<>();
    }

    public BrokerReportMap(BrokerReportMap that) {
        this.map = new HashMap<>(that.map);
    }

    public void update(String brokerID, LoadReport report) {
        // if the report already exists, the new one will replace the old one.
        map.put(brokerID, report);
    }

    public BrokerReportMap snapShot() {
        return new BrokerReportMap(this);
    }

    public ArrayList<LoadReport> toList() {
        ArrayList<LoadReport> res = new ArrayList<>();
        for (Map.Entry<String, LoadReport> entry : map.entrySet()) {
            res.add(entry.getValue());
        }
        return res;
    }
}
