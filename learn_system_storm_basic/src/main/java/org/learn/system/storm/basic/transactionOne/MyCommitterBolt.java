package org.learn.system.storm.basic.transactionOne;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;

/**
 * 最后的统计--更新数据库
 * @author zhangzuolong
 *
 */
public class MyCommitterBolt extends BaseTransactionalBolt implements ICommitter{
    
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final String GLOBAL_KEY="global_key";
	public static Map<String,DbValue> dbMap = new HashMap<String,DbValue>();
    int sum =0;
    TransactionAttempt attempt;
	public void execute(Tuple tuple) {
		sum +=Long.valueOf(tuple.getValueByField("count").toString());	
	}

	public void finishBatch() {
		DbValue db = dbMap.get(GLOBAL_KEY);
		DbValue newDb;
		if(db==null || !db.txid.equals(attempt.getTransactionId())){
			//更新数据库
			newDb =  new DbValue();
			newDb.txid = attempt.getTransactionId();
			if(db==null){
				newDb.count = sum;
			}else{
				newDb.count = db.count +sum;
			}
			dbMap.put(GLOBAL_KEY, newDb);
		}else{
			newDb = db;
		}
		System.out.println("total==================="+newDb.count);
	}

	public void prepare(Map arg0, TopologyContext arg1,
			BatchOutputCollector arg2, TransactionAttempt attempt) {
		this.attempt = attempt;	
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}
	
	public static class DbValue{
		BigInteger txid;
		int count;
	}

}
