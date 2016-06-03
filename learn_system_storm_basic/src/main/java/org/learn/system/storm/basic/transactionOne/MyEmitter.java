package org.learn.system.storm.basic.transactionOne;

import java.math.BigInteger;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Values;

public class MyEmitter implements ITransactionalSpout.Emitter<MyMata>{
	
	/**
	 * 构造函数
	 */
	Map<Long,String> dbMap = null;
	public MyEmitter(Map<Long,String> dbMap){
		this.dbMap = dbMap;
	}
	public void cleanupBefore(BigInteger arg0) {
		
	}

	public void close() {
		
	}
	
    /**
     * 逐个发射batch的tuple
     */
	public void emitBatch(TransactionAttempt attemptId, MyMata myMata,
			BatchOutputCollector coolector) {
		long beginPoint = myMata.getBeginPoint();
		int  num = myMata.getNum();
		for(long i=beginPoint;i<num+beginPoint;i++){
			//System.out.println("MyEmitter emitBatch========"+attemptId+"<--->"+dbMap.get(i));
			if(dbMap.get(i)== null){
				break;
			}else{
				coolector.emit(new Values(attemptId,dbMap.get(i)));
			}
			
		}
		
	}

}
