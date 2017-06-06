package edu.vanderbilt.chuilian.loadbalancer;

/**
 * Created by Killian on 6/5/17.
 */

import edu.vanderbilt.chuilian.types.LoadReport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * this analyzer will take broker report map to initialize then calculate critical statistics needed for channel plan generator
 */
// TODO: 6/5/17 TBD 
public class BrokerReportAnalyzer {
    ArrayList<LoadReport> reports;
    public BrokerReportAnalyzer(BrokerReportMap brokerReportMap) {
        this.reports = brokerReportMap.toList();
        this.reports.sort((LoadReport a, LoadReport b) -> (int) (a.loadRatio() - b.loadRatio()));
    }

    public Set<String> getLeastBusyBrokers(int numBrokers) {
        Set<String> res = new HashSet<>();
        for (int i = 0; i < numBrokers; i++) {
            res.add(reports.get(i).brokerID());
        }
        return res;
    }
}
