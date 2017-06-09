package edu.vanderbilt.chuilian.types;

import com.google.flatbuffers.FlatBufferBuilder;
import edu.vanderbilt.chuilian.loadbalancer.BrokerReport;
import edu.vanderbilt.chuilian.loadbalancer.ChannelReport;
import edu.vanderbilt.chuilian.loadbalancer.LoadAnalyzer;

import java.util.Map;

public class TypesBrokerReportHelper {

    public static byte[] serialize(BrokerReport brokerReport, long timeTag) {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        int[] channelRepts = new int[brokerReport.size()];
        int counter = 0;
        for (Map.Entry<String, ChannelReport> entry : brokerReport.entrySet()) {
            int topic = builder.createString(entry.getValue().getTopic());
            channelRepts[counter++] = TypesChannelReport.createTypesChannelReport(builder, topic, entry.getValue().getNumIOBytes(), entry.getValue().getNumIOMsgs(), entry.getValue().getNumPublications(), entry.getValue().getNumSubscribers());
        }
        int channelReports = TypesBrokerReport.createChannelReportsVector(builder, channelRepts);
        int brokerID = builder.createString(brokerReport.getBrokerID());
        TypesBrokerReport.startTypesBrokerReport(builder);
        TypesBrokerReport.addChannelReports(builder, channelReports);
        TypesBrokerReport.addBrokerID(builder, brokerID);
        TypesBrokerReport.addTimeTag(builder, timeTag);
        TypesBrokerReport.addLoadRatio(builder, brokerReport.getLoadRatio());
        TypesBrokerReport.addBandWidthBytes(builder, LoadAnalyzer.getBandWidthBytes());
        int report = TypesBrokerReport.endTypesBrokerReport(builder);
        builder.finish(report);
        java.nio.ByteBuffer buf = builder.dataBuffer();
        return builder.sizedByteArray();
    }

    public static TypesBrokerReport deserialize(byte[] data) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(data);
        return TypesBrokerReport.getRootAsTypesBrokerReport(buf);
    }

}
