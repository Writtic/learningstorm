/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 /**
  * Original is available at https://github.com/apache/storm/blob/master/examples/storm-starter/src/jvm/storm/starter/spout/RandomSentenceSpout.java
  */

package com.enow.example;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

//랜덤하게 문장들을 내뿜는 spout
public class RandomSentenceSpout extends BaseRichSpout {
  //아웃풋을 방출하는데 사용되는 콜렉터
  SpoutOutputCollector _collector;
  //난수 생성기
  Random _rand;

  //Open 함수는 클래스 인스턴스가 생성되면 호출된다.
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
  //콜렉터 인스턴스를 콜렉터에 넣어준다.
    _collector = collector;
    //난수 생성기
    _rand = new Random();
  }

  //스트림에 데이터를 방출
  @Override
  public void nextTuple() {
  //잠깐 쉬어주고 감
    Utils.sleep(100);
    //랜덤하게 방출될 sentences 문자열들
    String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
    //랜덤하게 한 문장을 뽐음
    String sentence = sentences[_rand.nextInt(sentences.length)];
    //문장 방출
    _collector.emit(new Values(sentence));
  }

  //Ack is not implemented since this is a basic example
  @Override
  public void ack(Object id) {
  }

  //Fail is not implemented since this is a basic example
  @Override
  public void fail(Object id) {
  }

  //Declare the output fields. In this case, an sentence
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }
}
