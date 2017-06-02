package edu.vanderbilt.chuilian.types;

import com.google.flatbuffers.FlatBufferBuilder;
import edu.vanderbilt.chuilian.loadbalancer.Plan;

/**
 * Created by Killian on 6/2/17.
 */
public class BalancerPlanHelper {
    public static byte[] serialize(Plan plan, long timeTag) {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        BalancerPlan.startBalancerPlan(builder);
        BalancerPlan.addTimeTag(builder, timeTag);
        int balancerPlan = BalancerPlan.endBalancerPlan(builder);
        builder.finish(balancerPlan);
        java.nio.ByteBuffer buf = builder.dataBuffer();
        return builder.sizedByteArray();
    }

    public static BalancerPlan deserialize(byte[] data) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(data);
        return BalancerPlan.getRootAsBalancerPlan(buf);
    }
}
