package com.microsoft.example;

import org.apache.storm.shade.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.apache.storm.spout.ShellSpout.LOG;

public class RandomTopicSpout extends BaseRichSpout {
    private ConcurrentHashMap<UUID, Values> _pending;
    SpoutOutputCollector _collector;
    Random _rand;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _pending = new ConcurrentHashMap<UUID, Values>();
        _collector = collector;
        _rand = new Random();
    }

    public void nextTuple() {
        try {
            UUID msgId = UUID.randomUUID();
            Utils.sleep(1000);
            String[] sentences = new String[]{
                    "trigger,enow/serverId/brokerId/deviceId1/2/1/1,trigger exec",
                    "status,enow/serverId/brokerId/deviceId1/2/1/1,status metadata"
            };
            String sentence = sentences[_rand.nextInt(sentences.length)];
            _pending.put(msgId, new Values(sentence));
            _collector.emit(new Values(sentence));
        }catch(Exception e){
            _collector.reportError(e);
            LOG.error("Spout error {}", e);
        }
    }

    public void ack(Object msgId) {
        _pending.remove(msgId);
    }

    public void fail(Object msgId) {
        _collector.emit(_pending.get(msgId), msgId);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("topicStructure"));
    }
}
