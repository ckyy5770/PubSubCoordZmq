package edu.vanderbilt.chuilian.loadbalancer.plan;

/**
 * Created by Killian on 6/5/17.
 */

public class HighLoadPlan extends SystemPlan {
    public HighLoadPlan(String from, String to, String channel) {
        super(from, to, channel);
    }

    public HighLoadPlan() {
        super();
    }
}