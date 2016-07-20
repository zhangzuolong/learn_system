package org.learn.system.storm.basic.hbase.state;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

/**
 * hbase 表的连接器
 * @author zhangzuolong
 *
 */
public class HTableConnector implements  Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Configuration configuration;
	private HTable table;
	private String tableName;
	
	/**
	 * 构造器
	 * @param conf
	 * @throws Exception
	 */
	public HTableConnector(TupleTableConfig conf) throws Exception{
		this.tableName = conf.getTableName();
		this.configuration = HBaseConfiguration.create();
		
		String filePath = "hbase-site.xml";
		Path path = new Path(filePath);
		this.configuration.addResource(path);
		this.table = new HTable(this.configuration, this.tableName);
	}
	
	public Configuration getConfiguration() {
		return configuration;
	}
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}
	public HTable getTable() {
		return table;
	}
	public void setTable(HTable table) {
		this.table = table;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public void close() throws IOException {
		this.table.close();
	}
	

}
