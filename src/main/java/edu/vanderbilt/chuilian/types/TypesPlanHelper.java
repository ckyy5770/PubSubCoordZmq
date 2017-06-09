package edu.vanderbilt.chuilian.types;

import com.google.flatbuffers.FlatBufferBuilder;
import edu.vanderbilt.chuilian.loadbalancer.plan.ChannelMapping;
import edu.vanderbilt.chuilian.loadbalancer.plan.ChannelPlan;
import edu.vanderbilt.chuilian.loadbalancer.plan.Plan;
import edu.vanderbilt.chuilian.loadbalancer.plan.Strategy;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Killian on 6/2/17.
 */
public class TypesPlanHelper {
    public static byte[] serialize(Plan plan, long timeTag) {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        ChannelMapping channelMapping = plan.getChannelMapping();
        int[] channelPlans = new int[channelMapping.size()];
        int counterChannel = 0;
        for (Map.Entry<String, ChannelPlan> entry : channelMapping.entrySet()) {
            int topicOffset = builder.createString(entry.getValue().getTopic());
            int strategyOffset = builder.createString(entry.getValue().getStrategy().toString());
            int[] availableBroker = new int[entry.getValue().getAvailableBroker().size()];
            int counterBroker = 0;
            for (String curBrokerID : entry.getValue().getAvailableBroker()) {
                availableBroker[counterBroker++] = builder.createString(curBrokerID);
            }
            int availableBrokerOffset = TypesChannelPlan.createAvailableBrokerVector(builder, availableBroker);
            channelPlans[counterChannel] = TypesChannelPlan.createTypesChannelPlan(builder, topicOffset, strategyOffset, availableBrokerOffset);
        }
        int channelMappingOffset = TypesPlan.createChannelMappingVector(builder, channelPlans);
        TypesPlan.startTypesPlan(builder);
        TypesPlan.addVersion(builder, plan.getVersion());
        TypesPlan.addTimeTag(builder, timeTag);
        TypesPlan.addChannelMapping(builder, channelMappingOffset);
        int Plan = TypesPlan.endTypesPlan(builder);
        builder.finish(Plan);
        ByteBuffer buf = builder.dataBuffer();
        return builder.sizedByteArray();
    }

    public static TypesPlan deserialize(byte[] data) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(data);
        return TypesPlan.getRootAsTypesPlan(buf);
    }

    public static Plan toPlan(TypesPlan typesPlan) {
        ChannelMapping channelMapping = new ChannelMapping();
        long version = typesPlan.version();
        int numChannel = typesPlan.channelMappingLength();
        for (int i = 0; i < numChannel; i++) {
            String topic = typesPlan.channelMapping(i).topic();
            String strategyString = typesPlan.channelMapping(i).strategy();
            Strategy strategy = null;
            switch (strategyString) {
                case "HASH":
                    strategy = Strategy.HASH;
                case "ALL_SUB":
                    strategy = Strategy.ALL_SUB;
                case "ALL_PUB":
                    strategy = Strategy.ALL_PUB;
            }
            Set<String> brokerIDs = new HashSet<>();
            int numBrokers = typesPlan.channelMapping(i).availableBrokerLength();
            for (int j = 0; j < numBrokers; j++) {
                brokerIDs.add(typesPlan.channelMapping(i).availableBroker(j));
            }
            ChannelPlan channelPlan = new ChannelPlan(topic, brokerIDs, strategy);
            channelMapping.addNewChannelPlan(channelPlan);
        }
        return new Plan(version, channelMapping);
    }
}
