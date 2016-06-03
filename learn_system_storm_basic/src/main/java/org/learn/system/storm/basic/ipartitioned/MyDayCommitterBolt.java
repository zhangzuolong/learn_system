package org.learn.system.storm.basic.ipartitioned;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyDayCommitterBolt extends BaseTransactionalBolt implements ICommitter{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
//	public static final String GLOBAL_KEY="global_key";
//	public static Map<String,DbValue> dbMap = new HashMap<String,DbValue>();
    int sum =0;
    TransactionAttempt attempt;
    String today;
    public static Map<String,Integer> countMap = new HashMap<String,Integer>();
    
	public void execute(Tuple tuple) {
		if(countMap.isEmpty()){
			System.out.println("countMap is Empty.....................................");
		}
	    today = tuple.getString(1);
	    Integer count = tuple.getInteger(2);
	    System.out.println("MyDayCommitterBolt execute========"+today+"<--->"+count+"<--->"+countMap.get(today));
	    if(countMap.containsKey(today)){
			count = countMap.get(today)+count;
		}	    
		countMap.put(today, count);
		System.out.println("MyDayCommitterBolt execute========"+today+"<--->"+count+"<--->"+countMap.get(today));
	}

	public void finishBatch() {
		Iterator<String> keys = countMap.keySet().iterator(); 
		while(keys.hasNext()) { 
			  String key = (String) keys.next(); 
			  Integer value=countMap.get(key); 
			  System.out.println("===================>>>"+key+"----"+value);
		 } 
//		DbValue db = dbMap.get(GLOBAL_KEY);
//		DbValue newDb;
//		if(db==null || !db.txid.equals(attempt.getTransactionId())){
//			//更新数据库
//			newDb =  new DbValue();
//			newDb.txid = attempt.getTransactionId();
//			if(db==null){
//				newDb.count = sum;
//			}else{
//				newDb.count = db.count +sum;
//			}
//			dbMap.put(GLOBAL_KEY, newDb);
//		}else{
//			newDb = db;
//		}
//		System.out.println("total==================="+newDb.count);
	}

	public void prepare(Map arg0, TopologyContext arg1,
			BatchOutputCollector arg2, TransactionAttempt attempt) {
		this.attempt = attempt;			
		System.out.println("--------------------------------------------------------");
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		
	}

}
