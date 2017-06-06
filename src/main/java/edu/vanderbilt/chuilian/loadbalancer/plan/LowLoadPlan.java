package edu.vanderbilt.chuilian.loadbalancer.plan;

/**
 * Created by Killian on 6/5/17.
 */
public class LowLoadPlan extends SystemPlan {
    public LowLoadPlan(String from, String to, String channel) {
        super(from, to, channel);
    }

    public LowLoadPlan() {
        super();
    }
}
