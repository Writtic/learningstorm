package com.enow.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.enow.example.RandomSentenceSpout;

public class WordCountTopology {

  //Entry point for the topology
  //토폴로지를 위한 진입점
  public static void main(String[] args) throws Exception {
  //Used to build the topology
  //토폴로지 빌더
    TopologyBuilder builder = new TopologyBuilder();
    //Add the spout, with a name of 'spout'
    //and parallelism hint of 5 executors
    //'spout'이란 이름과 함께, spout를 추가한다.
    //그리고 익스큐터(스레드)는 5개부터 시작한다.
    builder.setSpout("spout", new RandomSentenceSpout(), 5);
    //Add the SplitSentence bolt, with a name of 'split'
    //and parallelism hint of 8 executors
    //shufflegrouping subscribes to the spout, and equally distributes
    //tuples (sentences) across instances of the SplitSentence bolt
    //'split'이란 이름과 함께, SplitSentence bolt를 추가한다.
    //초기 익스큐터(스레드)는 8개부터 시작한다.
    //shufflegrouping은 해당 spout를 구독한다.
    //그리고 SplitSentece 볼트의 인스턴스를 건너 튜플들(문장들)을 동등하게 분배한다.
    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    //Add the counter, with a name of 'count'
    //and parallelism hint of 12 executors
    //fieldsgrouping subscribes to the split bolt, and
    //ensures that the same word is sent to the same instance (group by field 'word')
    //'count'라는 이름으로 counter를 추가한다.
    //초기 익스큐터는 12개
    //fieldgrouping은 split 볼트를 구독한다.
    //그리고 같은 단어가 똑같은 단어로 그룹핑된 인스턴스로 보내지는지 확인한다.
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    //new configuration
    //새로운 설정
    Config conf = new Config();
    conf.setDebug(true);

    //If there are arguments, we are running on a cluster
    //커멘트에서 추가로 들어오는 값들이 있다면 cluster 상에서 돌아가게 된다.
    if (args != null && args.length > 0) {
      //parallelism hint to set the number of workers
      //worder의 수를 설정하기 위한 parallelism hint
      conf.setNumWorkers(3);
      //submit the topology
      //토폴로지 서브밋
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    //Otherwise, we are running locally
    //그렇지 않으면 로컬에서 돌림
    else {
      //Cap the maximum number of executors that can be spawned
      //for a component to 3
      //스폰될 수 있는 최대 익스큐터를 제한한다.
      //컴포넌트당 3개?
      conf.setMaxTaskParallelism(3);
      //LocalCluster is used to run locally
      //LocalCluster은 로컬에서 돌리는데 사용된다.
      LocalCluster cluster = new LocalCluster();
      //submit the topology
      //토폴로지 서밋
      cluster.submitTopology("word-count", conf, builder.createTopology());
      //sleep
      //잠듬
      Thread.sleep(10000);
      //shut down the cluster
      //클러스터 종료
      cluster.shutdown();
    }
  }
}
