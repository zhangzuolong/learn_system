package org.learn.system.storm.basic.transactionOne;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.transactional.TransactionalTopologyBuilder;

/**
 * 普通事物案例分析
 * @author zhangzuolong
 *
 */
public class MyTopo {

	public static void main(String[] args) {
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("ttbId", "spoutId", new MyTxSpout(),1);
		builder.setBolt("myTrans", new MyTransactionBolt(), 3).shuffleGrouping("spoutId");
		builder.setBolt("myCommiter", new MyCommitterBolt(), 1).shuffleGrouping("myTrans");
		
		Config conf = new Config() ;
		conf.setDebug(false);
		conf.setNumWorkers(2);
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.buildTopology());
			}  catch (Exception e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.buildTopology());
		}

	}

}
