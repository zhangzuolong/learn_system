package org.learn.system.storm.basic.filedemo;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MyBolt extends BaseRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	OutputCollector collector=null;
	
	int num=0;
	String valueStr = null;
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
	    this.collector = collector;
		
	}
	public void execute(Tuple tuple) {
		try{
			valueStr = tuple.getValueByField("log").toString();
			if(null!=valueStr){
				num ++;
				System.err.println(Thread.currentThread().getName()+"   lines  :"+num +"   session_id:"+valueStr.split("\t")[1]);	
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}


}
