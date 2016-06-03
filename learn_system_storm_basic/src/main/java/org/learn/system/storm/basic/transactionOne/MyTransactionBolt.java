package org.learn.system.storm.basic.transactionOne;

import java.util.Map;





import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyTransactionBolt extends BaseTransactionalBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    
	long count=0;
	BatchOutputCollector collector;
	TransactionAttempt attempt;
	/**
	 * 每个tuple处理
	 */
	public void execute(Tuple tuple) {
		attempt = (TransactionAttempt)tuple.getValue(0);
		System.out.println("MyTransactionBolt transactionAttempt="+attempt.getAttemptId()+"--"+attempt.getTransactionId());
	    String log = tuple.getString(1);
	    if(log!=null && log.split("\t").length>0){
	    	count ++;
	    }
	}
    
	/**
	 * 每个批次处理一遍
	 */
	public void finishBatch() {
		System.out.println("MyTransactionBolt finishBatch 此批次处理完毕,count==="+count);
		collector.emit(new Values(attempt,count));
	}

	public void prepare(Map arg0, TopologyContext arg1,
			BatchOutputCollector collector, TransactionAttempt attempt) {
		System.out.println("MyTransactionBolt prepare="+attempt.getAttemptId()+"--"+attempt.getTransactionId());
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer decalarer) {
		decalarer.declare(new Fields("tx","count"));
		
	}

}
