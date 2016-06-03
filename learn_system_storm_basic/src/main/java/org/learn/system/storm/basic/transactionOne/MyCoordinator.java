package org.learn.system.storm.basic.transactionOne;

import java.math.BigInteger;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.utils.Utils;

public class MyCoordinator implements ITransactionalSpout.Coordinator<MyMata>{
	/**batch 的个数*/
	public static int BATCH_NUM=10;
	
	public void close() {
		
	}
    /**创建一个新的metadata，当isReady() 为true时，发射该metadata
     * （事务tuple）到“batch emit”流
    */
	/**
	 * txid:事物ID
	 * prevMetadata:上一个元数据
	 */
	public MyMata initializeTransaction(BigInteger txid, MyMata prevMetadata) {
		System.out.println("初始化开始...");
		long beginPoint = 0;
		if(prevMetadata == null){
			beginPoint = 0;
		}else{
			beginPoint = prevMetadata.getBeginPoint()+prevMetadata.getNum();
		}
		MyMata  mata = new MyMata();
		mata.setBeginPoint(beginPoint);
		mata.setNum(BATCH_NUM);
		System.out.println("启动一个事物："+mata.toString());
		return mata;
	}
    /**
     * 为true时启动新事务，需要时可以在此sleep
     */
	public boolean isReady() {
	    Utils.sleep(2000);
		return true;
	}

}
