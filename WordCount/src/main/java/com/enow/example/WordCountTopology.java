package com.enow.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
// import backtype.storm.Config;
// import backtype.storm.LocalCluster;
// import backtype.storm.generated.StormTopology;
// import backtype.storm.tuple.Fields;

import com.enow.example.RandomSentenceSpout;

public class WordCountTopology {
  //토폴로지를 위한 진입점
  public static void main(String[] args) throws Exception {
  //토폴로지 빌더
    TopologyBuilder builder = new TopologyBuilder();
    //'spout'이란 이름과 함께, spout를 추가한다.
    //그리고 익스큐터(스레드)는 5개부터 시작한다.
    builder.setSpout("spout", new RandomSentenceSpout(), 5);
    //'split'이란 이름과 함께, SplitSentence bolt를 추가한다.
    //초기 익스큐터(스레드)는 8개부터 시작한다.
    //shufflegrouping은 해당 spout를 구독한다.
    //그리고 SplitSentece 볼트의 인스턴스를 건너 튜플들(문장들)을 동등하게 분배한다.
    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    //'count'라는 이름으로 counter를 추가한다.
    //초기 익스큐터는 12개
    //fieldgrouping은 split 볼트를 구독한다.
    //그리고 같은 단어가 똑같은 단어로 그룹핑된 인스턴스로 보내지는지 확인한다.
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    //새로운 설정
    Config conf = new Config();
    conf.setDebug(true);

    //커멘트에서 추가로 들어오는 값들이 있다면 cluster 상에서 돌아가게 된다.
    if (args != null && args.length > 0) {
      //worder의 수를 설정하기 위한 parallelism hint
      conf.setNumWorkers(3);
      //토폴로지 서브밋
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    //그렇지 않으면 로컬에서 돌림
    else {
      //스폰될 수 있는 최대 익스큐터를 제한한다.
      //컴포넌트당 3개?
      conf.setMaxTaskParallelism(3);
      //LocalCluster은 로컬에서 돌리는데 사용된다.
      LocalCluster cluster = new LocalCluster();
      //토폴로지 서밋
      cluster.submitTopology("word-count", conf, builder.createTopology());
      //잠듬
      Thread.sleep(10000);
      //클러스터 종료
      cluster.shutdown();
    }
  }
}
