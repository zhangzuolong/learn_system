package org.learn.system.storm.basic.transactionOne;

import java.io.Serializable;

/**
 * 元数据
 * @author zhangzuolong
 *
 */
public class MyMata implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**开始节点*/
	private long beginPoint;
	/**batch的tuple个数*/
	private int  num;
	
	public long getBeginPoint() {
		return beginPoint;
	}
	public void setBeginPoint(long beginPoint) {
		this.beginPoint = beginPoint;
	}
	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}
	@Override
	public String toString() {
		return "beginPoint="+this.beginPoint+"<--->num"+this.num;
	}
}
