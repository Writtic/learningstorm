package com.microsoft.example;
/**
 * Created by writtic on 2016. 8. 22..
 */


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

public class triggerTestTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomTopicSpout(), 1);
        builder.setBolt("scheduling", new SchedulingBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("provisioning", new ProvisioningBolt(), 1).fieldsGrouping("scheduling", new Fields("topicStructure"));
        Config conf = new Config();
        conf.setDebug(true);
        if (args != null && args.length > 0) {
            List<String> nimbus_seeds = new ArrayList<String>();
            nimbus_seeds.add("192.168.99.100");
            nimbus_seeds.add("172.17.0.4");
            List<String> zookeeper_servers = new ArrayList<String>();
            zookeeper_servers.add("192.168.99.100");
            zookeeper_servers.add("172.17.0.2");
            conf.put(Config.NIMBUS_SEEDS, nimbus_seeds);
            conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
            conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            conf.put(Config.STORM_ZOOKEEPER_SERVERS, zookeeper_servers);
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.killTopology("word-count");
            cluster.shutdown();
        }
    }
}