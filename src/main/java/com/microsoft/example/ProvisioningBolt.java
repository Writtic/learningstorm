package com.microsoft.example;

/**
 * Created by writtic on 2016. 8. 22..
 */
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

// For logging
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


public class ProvisioningBolt extends BaseBasicBolt {

    private static final Logger _LOG = LogManager.getLogger(ProvisioningBolt.class);
    public void execute(Tuple input, BasicOutputCollector collector) {

        String temp = input.toString().substring(input.toString().indexOf("[")+1, input.toString().length() - 1);
        if ((null == temp) || (temp.length() == 0)) {
            _LOG.warn("input value or length of input is empty : [" + input + "]\n");
            return;
        }
        String spoutName = temp.split(",",3)[0];
        String msgs = temp.split(",",3)[2];
        collector.emit(new Values(input));
        _LOG.info("Emitting a message of " + spoutName + " : " + msgs + "\n");
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("topicStructure"));
    }
}
