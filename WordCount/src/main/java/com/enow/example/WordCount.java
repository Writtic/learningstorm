package com.enow.example;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
//다양한 볼트 타입이 있지만, 우리는 BaseBasicBolt를 사용할 것이다.
public class WordCount extends BaseBasicBolt {
    //For holding words and counts
    //단어들을 홀딩하고 카운트
    Map<String, Integer> counts = new HashMap<String, Integer>();

    //execute is called to process tuples
    //execute 함수는 튜플을 수행하기위해 호출된다.
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      //Get the word contents from the tuple
      //튜플로부터 단어 내용물들을 가져온다.
      String word = tuple.getString(0);
      //Have we counted any already?
      //우리가 뭘 카운트하긴 했던가?
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      //Increment the count and store it
      //카운트 ++하고 저장한다.
      count++;
      counts.put(word, count);
      //Emit the word and the current count
      //단어와 현재 카운트를 방출
      collector.emit(new Values(word, count));
    }

    //Declare that we will emit a tuple containing two fields; word and count
    //두가지 필드르 ㄹ 포함하는 튜플을 방충하겠다고 선언한다; word와 count
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }
