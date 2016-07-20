package org.learn.system.storm.basic.hbase.state;

import java.util.Random;

import org.learn.system.storm.basic.trident.functions.MySplit;
import org.learn.system.storm.basic.trident.functions.Split;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 用Trident实现汇总型PV统计： 按天 累计
 * 
 * @author zhangzuolong
 *
 */
public class TridentPVTopo {

	public static StormTopology buildTopology(LocalDRPC drpc) {
		// 构造数据源
		String[] hosts = { "www.taobao.com" };
		String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123","XXYH6YCGFJYERTT834R52FDXV9U34",
				"BBYH61456FGHHJ7JL89RG5VV9UYU7", "CYYH6Y2345GHI899OFG4V9U567","VVVYH6Y4V4SFXZ56JIPDPB4V678" };
		String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51",
				"2014-01-07 08:40:52", "2014-01-07 08:40:53",
				"2014-01-08 09:40:49", "2014-04-07 10:40:49",
				"2014-03-09 11:40:49", "2014-01-07 12:40:49" };
		Random random = new Random();
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("eachlog"), 3,
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 				
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]),
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 				
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]), 
				new Values(hosts[0] + "\t" + session_id[random.nextInt(5)]+ "\t" + time[random.nextInt(8)]));
		// 是否循环
		spout.setCycle(false);
		// 定义topo
		TridentTopology topology = new TridentTopology();
		TridentState pvcounts = topology.newStream("spout", spout)
				// .parallelismHint(16)
				.each(new Fields("eachlog"), new MySplit("\t"),new Fields("date", "session_id"))
				.groupBy(new Fields("date"))
				.persistentAggregate(new MemoryMapState.Factory(),new Fields("session_id"), new Count(), new Fields("PV"))
		        // .parallelismHint(16)
		        ;
		topology.newDRPCStream("getPV", drpc)
				.each(new Fields("args"), new Split(" "), new Fields("date"))
				.groupBy(new Fields("date"))
				.stateQuery(pvcounts, new Fields("date"), new MapGet(),new Fields("PV"))
				.each(new Fields("PV"), new FilterNull())
		        // .aggregate(new Fields("count"), new Sum(), new Fields("sum"))
		        ;
		return topology.build();

	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("pvCount", conf, buildTopology(drpc));
			for (int i = 0; i < 100; i++) {
				System.err.println("DRPC RESULT: "
						+ drpc.execute("getPV", "2014-01-07 2014-01-08 2014-03-09 2014-04-07"));
				Thread.sleep(1000);
			}
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
		}
	}

}
