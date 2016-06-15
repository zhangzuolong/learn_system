package org.learn.system.storm.basic.trident.functions;

import org.learn.system.storm.basic.tools.DateFmt;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class MySplit extends BaseFunction{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String patton;
	
    public MySplit(String patton) {
    	this.patton = patton;
	}

	//对每一个传入的进行分割处理
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String log = tuple.getString(0);
		String[] logInfo = log.split(patton);
		if(logInfo.length>=3){
			collector.emit(new Values(DateFmt.getCountDate(logInfo[2], DateFmt.date_short),logInfo[1]));
		}
	}

}
