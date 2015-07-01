package storm.starter.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;



/**
 * Created by wanluwang on 01/07/2015.
 */
public class TridentHelloWorld {

    public static class QueryParser extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            sentence += " Ontoit!! ";
            collector.emit(new Values(sentence));
        }
    }

    public static class QueryHandler extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            sentence += " Cambridge!!! ";
            collector.emit(new Values(sentence));
        }
    }


    public static StormTopology buildTopology(LocalDRPC ldrpc) {
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("query", ldrpc).each(new Fields("args"), new QueryParser(), new Fields("parser"))
                .each(new Fields("parser"), new QueryHandler(), new Fields("handler"))
                .project(new Fields("handler"));
//                .groupBy(new Fields("handler"));
//                .aggregate(new Fields("one"), new Sum(), new Fields("reach"));
        return topology.build();
    }

    public static void main(String[] argv) throws Exception {
        LocalDRPC ldrpc = new LocalDRPC();
        Config config = new Config();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("hello_world", config, buildTopology(ldrpc));
        for (int i = 0; i < 10; i++) {
            String results = ldrpc.execute("query", "Hello World!");
            System.out.println("DRPC RESULT" + i + " : " + results);
            Thread.sleep(2000);
        }
        cluster.shutdown();
        ldrpc.shutdown();
    }
}
