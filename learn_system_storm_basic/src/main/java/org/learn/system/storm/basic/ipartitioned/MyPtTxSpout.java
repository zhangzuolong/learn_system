package org.learn.system.storm.basic.ipartitioned;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.learn.system.storm.basic.transactionOne.MyMata;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 分区事物spout
 * @author zhangzuolong
 *
 */
public class MyPtTxSpout implements IPartitionedTransactionalSpout<MyMata>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**batch 的个数*/
	public static int BATCH_NUM=10;

	public Map<Integer,Map<Long,String>> PT_DATA_MP = new HashMap<Integer,Map<Long,String>>();
	/**
	 * 构造函数--创造数据源
	 */
	public MyPtTxSpout(){		
		Random random = new Random();
		String[] hosts = { "www.taobao.com" };
		String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
				"CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
		String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53", 
				"2014-01-07 09:40:49", "2014-04-07 10:40:49", "2014-03-07 11:40:49", "2014-01-07 12:40:49" };
		for(int j=0;j<5;j++){
			Map<Long,String> dbMap = new HashMap<Long,String>();
			for(long i=0;i<100;i++){
				dbMap.put(i,hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]+"\n");
			}
			PT_DATA_MP.put(j, dbMap);
		}
		
	}
	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx","log"));
		
	}

	public Map<String, Object> getComponentConfiguration() {		
		return null;
	}

	public backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout.Coordinator getCoordinator(
			Map arg0, TopologyContext arg1) {
		return new MyCoordinator();
	}

	public backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout.Emitter<MyMata> getEmitter(
			Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		return new MyEmitter();
	}
	
	/**
	 * 内部类--MyEmitter(发射器-发射tuple)
	 * @author zhangzuolong
	 *
	 */
	public class MyEmitter implements IPartitionedTransactionalSpout.Emitter<MyMata>{		
		/**
		 * 关闭--清空
		 */
		public void close() {			
			
		}
        
		/**
		 * 发射PartitionBatch
		 */
		public void emitPartitionBatch(TransactionAttempt attempt,BatchOutputCollector collector, int partition, MyMata myMata) {
			long beginPoint = myMata.getBeginPoint();
			int  num = myMata.getNum();
			Map<Long,String> dbMap = PT_DATA_MP.get(partition);
			for(long i=beginPoint;i<num+beginPoint;i++){
				//System.out.println("MyEmitter emitBatch========"+attemptId+"<--->"+dbMap.get(i));
				if(dbMap.get(i)== null){
					break;
				}else{
					System.out.println("MyEmitter emitPartitionBatch 发射数据-->"+attempt+dbMap.get(i));
					collector.emit(new Values(attempt,dbMap.get(i)));
				}
				
			}
		}
        
		/**
		 * 启动新的PartitionBatch
		 */
		public MyMata emitPartitionBatchNew(TransactionAttempt attempt,
				BatchOutputCollector collector, int partition, MyMata lastMyMata) {
			System.out.println("初始化开始...");
			long beginPoint = 0;
			if(lastMyMata == null){
				beginPoint = 0;
			}else{
				beginPoint = lastMyMata.getBeginPoint()+lastMyMata.getNum();
			}
			MyMata  mata = new MyMata();
			mata.setBeginPoint(beginPoint);
			mata.setNum(BATCH_NUM);
			System.out.println("MyEmitter emitPartitionBatchNew 启动一个新的PartitionBatch："+mata.toString());
			//发射
			emitPartitionBatch(attempt,collector,partition,mata);
			return mata;
		}
    }
	
	/**
	 * 协调构造器
	 * @author zhangzuolong
	 *
	 */
	public class MyCoordinator implements IPartitionedTransactionalSpout.Coordinator{

		public void close() {
			
		}
        
		/**
		 * 是否启动新事物--true:启动
		 */
		public boolean isReady() {
			Utils.sleep(2000);
			return true;
		}
        
		/**
		 * 得到分区数
		 */
		public int numPartitions() {
			System.out.println("MyCoordinator numPartitions 得到分区数为-->"+5);
			return 5;
		}
		
	}
	
}
	
	