package org.learn.system.storm.basic.hbase.state;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
/**
 * hbase 表结构
 * @author zhangzuolong
 *
 */
public class TupleTableConfig implements  Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**hbase表名*/
	private String  tableName;
	/**hbase rowKey */
	private String  tupleRowKeyField;
    /**hbase 时间戳*/
	private String  tupleTimestampField;
	/**hbase 列族 */
	private Map<String,Set<String>> columnFamilies;
	
	public TupleTableConfig(String tableName, String tupleRowKeyField) {
		super();
		this.tableName = tableName;
		this.tupleRowKeyField = tupleRowKeyField;
		this.tupleTimestampField="";
		this.columnFamilies = new HashMap<String,Set<String>>();
	}
	public TupleTableConfig(String tableName, String tupleRowKeyField,
			String tupleTimestampField) {
		super();
		this.tableName = tableName;
		this.tupleRowKeyField = tupleRowKeyField;
		this.tupleTimestampField = tupleTimestampField;
		this.columnFamilies = new HashMap<String,Set<String>>();
	}
	
	public TupleTableConfig(String tableName, String tupleRowKeyField,
			String tupleTimestampField, Map<String, Set<String>> columnFamilies) {
		super();
		this.tableName = tableName;
		this.tupleRowKeyField = tupleRowKeyField;
		this.tupleTimestampField = tupleTimestampField;
		this.columnFamilies = columnFamilies;
	}
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getTupleRowKeyField() {
		return tupleRowKeyField;
	}
	public void setTupleRowKeyField(String tupleRowKeyField) {
		this.tupleRowKeyField = tupleRowKeyField;
	}
	public String getTupleTimestampField() {
		return tupleTimestampField;
	}
	public void setTupleTimestampField(String tupleTimestampField) {
		this.tupleTimestampField = tupleTimestampField;
	}
	public Map<String, Set<String>> getColumnFamilies() {
		return columnFamilies;
	}
	public void setColumnFamilies(Map<String, Set<String>> columnFamilies) {
		this.columnFamilies = columnFamilies;
	}
	
	

}
