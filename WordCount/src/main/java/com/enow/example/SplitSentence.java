package com.enow.example;

import java.text.BreakIterator;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//There are a variety of bolt types. In this case, we use BaseBasicBolt
//다양한 볼트 타입이 있지만, 우리는 BaseBasicBolt를 사용할 것이다.
public class SplitSentence extends BaseBasicBolt {

  //Execute is called to process tuples
  //Execute는 튜플을 실행하기 위해 호출된다.
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    //Get the sentence content from the tuple
    //튜플로부터 문장 내용물을 가져온다.
    String sentence = tuple.getString(0);
    //An iterator to get each word
    //각각 단어를 가져올 iterator
    BreakIterator boundary=BreakIterator.getWordInstance();
    //Give the iterator the sentence
    //문장들을 iterator에게 준다.
    boundary.setText(sentence);
    //Find the beginning first word
    //시작 단어를 찾는다.
    int start=boundary.first();
    //Iterate over each word and emit it to the output stream
    //각각 단어상에서 반복하고 아웃풋 스트림에 방출한다.
    for (int end=boundary.next(); end != BreakIterator.DONE; start=end, end=boundary.next()) {
      //get the word
      //방출된 단어를 가져온다.
      String word=sentence.substring(start,end);
      //If a word is whitespace characters, replace it with empty
      //만약 단어가 공백문자들이면 비워준다.
      word=word.replaceAll("\\s+","");
      //if it's an actual word, emit it
      //만약 이게 실제 한 단어면 방충한다.
      if (!word.equals("")) {
        collector.emit(new Values(word));
      }
    }
  }

  //Declare that emitted tuples will contain a word field
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }
}
