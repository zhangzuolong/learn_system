package org.learn.system.storm.basic.ipartitioned;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.learn.system.storm.basic.tools.DateFmt;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyDayBatchBolt  implements IBatchBolt<TransactionAttempt>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	TransactionAttempt attempt;
    Integer count = null;
    String today="";
    Map<String,Integer> countMap = new HashMap<String,Integer>();
    BatchOutputCollector collector;
    
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx","date","count"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void execute(Tuple tuple) {
		String log = tuple.getStringByField("log");
		attempt = (TransactionAttempt)tuple.getValue(0);
		if(log!=null && log.split("\t").length>=3){
			today = DateFmt.getCountDate(log.split("\t")[2],DateFmt.date_short);
			if(countMap.containsKey(today)){
				count = countMap.get(today)+1;
			}else{
				count = 1;
			}
			countMap.put(today, count);
		}
	}

	public void finishBatch() {
		if(!countMap.isEmpty()){
			 Iterator<String> keys = countMap.keySet().iterator(); 
			 while(keys.hasNext()) { 
				  String key = (String) keys.next(); 
				  Integer value=countMap.get(key); 
				  System.out.println("MyDayBatchBolt finishBatch===================>>>"+attempt.getTransactionId()+"---"+key+"----"+value);
				  collector.emit(new Values(attempt,key,value));
			 } 
		     countMap.clear();
		}
		
	}

	public void prepare(Map arg0, TopologyContext arg1,
			BatchOutputCollector collector, TransactionAttempt attempt) {
		this.collector = collector;
		
	}

}
