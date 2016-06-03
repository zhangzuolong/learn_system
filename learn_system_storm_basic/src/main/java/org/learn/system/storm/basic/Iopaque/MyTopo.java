package org.learn.system.storm.basic.Iopaque;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.transactional.TransactionalTopologyBuilder;

public class MyTopo {

	public static void main(String[] args) {
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("ttbId", "spoutId", new MyOpaquePtTxSpout(),1);
		builder.setBolt("myBatch", new MyDayBatchBolt(), 3).shuffleGrouping("spoutId");
		builder.setBolt("myCommiter", new MyDayCommitterBolt(), 1).shuffleGrouping("myBatch");
		
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
