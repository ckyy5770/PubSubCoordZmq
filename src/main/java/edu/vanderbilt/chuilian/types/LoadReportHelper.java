package edu.vanderbilt.chuilian.types;

import com.google.flatbuffers.FlatBufferBuilder;
import edu.vanderbilt.chuilian.loadbalancer.ChannelReport;
import edu.vanderbilt.chuilian.loadbalancer.ReportMap;

import java.util.Map;

public class LoadReportHelper {

    public static byte[] serialize(ReportMap reportMap, long timeTag) {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        int[] channelRepts = new int[reportMap.size()];
        int counter = 0;
        for (Map.Entry<String, ChannelReport> entry : reportMap.entrySet()) {
            int topic = builder.createString(entry.getValue().getTopic());
            channelRepts[counter++] = ReportEntry.createReportEntry(builder, topic, entry.getValue().getNumIOBytes(), entry.getValue().getNumIOMsgs(), entry.getValue().getNumPublications(), entry.getValue().getNumSubscribers());
        }
        int channelReports = LoadReport.createChannelReportsVector(builder, channelRepts);
        int brokerID = builder.createString(reportMap.getBrokerID());
        LoadReport.startLoadReport(builder);
        LoadReport.addChannelReports(builder, channelReports);
        LoadReport.addBrokerID(builder, brokerID);
        LoadReport.addTimeTag(builder, timeTag);
        LoadReport.addLoadRatio(builder, reportMap.getLoadRatio());
        int report = LoadReport.endLoadReport(builder);
        builder.finish(report);
        java.nio.ByteBuffer buf = builder.dataBuffer();
        return builder.sizedByteArray();
    }

    public static LoadReport deserialize(byte[] data) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(data);
        return LoadReport.getRootAsLoadReport(buf);
    }

}
