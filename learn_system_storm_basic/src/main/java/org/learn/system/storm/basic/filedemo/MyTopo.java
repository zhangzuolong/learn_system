package org.learn.system.storm.basic.filedemo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;


public class MyTopo {

	public static void main(String[] args) {
		System.out.println("11111111111");
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("mySpolt", new MySpolt(),1);
		builder.setBolt("myBolt", new MyBolt(), 2).shuffleGrouping("mySpolt");
		
		Config conf = new Config() ;
		conf.setDebug(false);
		conf.setNumWorkers(2);
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			}  catch (Exception e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.createTopology());
		}

	}

}
