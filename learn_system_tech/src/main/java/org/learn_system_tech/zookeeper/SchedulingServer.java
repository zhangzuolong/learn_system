package org.learn_system_tech.zookeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class SchedulingServer implements Watcher{
	private ZooKeeper zooKeeper;
	private String connectString;  //conectString连接字符串，包括IP地址，服务器端口号 
	private int sessionTimeout;
	
	public void intConf() throws Exception {
		InitConfReader reader = new InitConfReader();
		List<String> keys = new ArrayList<String>();
		keys.add("connectString");
		keys.add("sessionTimeout");
		Map<String, String> confs = reader.getConfs(keys);
		this.connectString = confs.get("connectString");
		this.sessionTimeout = Integer.parseInt(confs.get("sessionTimeout"));
		zooKeeper = new ZooKeeper(connectString, sessionTimeout, this);
	}
	/*
	1.整个系统中的所有静态节点均被创建。
	2.“/root”节点被创建，“/root/client”未被创建
	3.“/root/client”被创建但其下子节点一个或多个未被创建.
	4.存储状态的一个或多个节点未被创建
	*/ 
		
	public void initServer() throws Exception {
		//stat用于存储被监测节点是否存在，若不存在则对应的值为null 
		Stat stat = zooKeeper.exists("/root", false);
		if(stat == null) {
			zooKeeper.create("/root", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zooKeeper.create("/root/error", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zooKeeper.create("/root/processed", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zooKeeper.create("/root/wait", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zooKeeper.create("/root/temp", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		stat = zooKeeper.exists("/root/error",false);
		if(stat == null) {
			zooKeeper.create("/root/error", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		stat = zooKeeper.exists("/root/processed",false);
		if(stat == null) {
			zooKeeper.create("/root/processed", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		stat = zooKeeper.exists("/root/wait",false);
		if(stat == null) {
			zooKeeper.create("/root/wait", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		stat = zooKeeper.exists("/root/temp",false);
		if(stat == null) {
			zooKeeper.create("/root/temp", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}
	
	public void process(WatchedEvent event){
	}

}
