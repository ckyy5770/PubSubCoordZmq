package edu.vanderbilt.chuilian.loadbalancer.plan;

/**
 * Created by Killian on 6/5/17.
 */
public enum Strategy {
    HASH("HASH"), ALL_SUB("ALL_SUB"), ALL_PUB("ALL_PUB");

    private final String strategyStr;

    Strategy(final String strategyStr) {
        this.strategyStr = strategyStr;
    }

    @Override
    public String toString() {
        return strategyStr;
    }
}