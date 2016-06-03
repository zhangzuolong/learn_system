package org.learn.system.storm.basic.transactionOne;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.tuple.Fields;

/**
 * 继承普通事物Spout
 * @author zhangzuolong
 *
 */
public class MyTxSpout implements ITransactionalSpout<MyMata>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 构造函数--数据源
	 */
	Map<Long,String> dbMap = null;
	public MyTxSpout(){
		dbMap = new HashMap<Long,String>();
		Random random = new Random();
		String[] hosts = { "www.taobao.com" };
		String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
				"CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
		String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53", 
				"2014-01-07 09:40:49", "2014-04-07 10:40:49", "2014-03-07 11:40:49", "2014-01-07 12:40:49" };
		for(long i=0;i<100;i++){
			dbMap.put(i,hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]+"\n");
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx","log"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public backtype.storm.transactional.ITransactionalSpout.Coordinator<MyMata> getCoordinator(
			Map arg0, TopologyContext arg1) {
		return new MyCoordinator();
	}

	public backtype.storm.transactional.ITransactionalSpout.Emitter<MyMata> getEmitter(
			Map arg0, TopologyContext arg1) {
		return new MyEmitter(dbMap);
	}

}
