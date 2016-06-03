package org.learn.system.storm.basic.filedemo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;



public class MySpolt  extends BaseRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector collector=null;  
	FileInputStream fis;
	InputStreamReader isr;
	BufferedReader br;	
	String str=null;
	
	/**
	 * 数据初始化
	 */
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
		try {
			this.collector = collector;
			this.fis = new FileInputStream("track.log");
			this.isr = new InputStreamReader(fis, "UTF-8");
			this.br = new BufferedReader(isr);
		}  catch (Exception e) {
			e.printStackTrace();
		}
		
	}
    /**
     * tuple
     */
	public void nextTuple() {
		try {
			while((str = this.br.readLine()) !=null){
				collector.emit(new Values(str));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));
		
	}
    
	/**
	 * 关闭
	 */
	@Override
	public void close() {
		try {
			br.close();
			isr.close();
			fis.close();
			super.close();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
	}
	
}
