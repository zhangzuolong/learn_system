package org.learn.system.storm.basic.trident.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class Split extends BaseFunction{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	String patten;
	public Split(String patten) {
		this.patten = patten;
	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String tupleInfo = tuple.getString(0);
		for(String info:tupleInfo.split(patten)){
			collector.emit(new Values(info));
		}
	}
	
}
