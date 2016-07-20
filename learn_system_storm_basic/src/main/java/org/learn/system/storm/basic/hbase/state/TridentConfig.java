package org.learn.system.storm.basic.hbase.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.Serializer;
import storm.trident.state.StateType;
import sun.security.action.PutAllAction;
/**
 * Trident 配置
 * @author zhangzuolong
 *
 * @param <T>
 */
@SuppressWarnings("rawtypes")
public class TridentConfig<T> extends TupleTableConfig {	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int stateCacheSize = 1000;
	private Serializer stateSerializer;
	
	/**
	 * 定义，非事务，事务，不透明事务的机制。
	 */
	public static final Map<StateType,Serializer> DEFAULT_SERIALIZES 
	    = new HashMap<StateType,Serializer>(){
		{
			put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
			put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
			put(StateType.OPAQUE, new JSONOpaqueSerializer());
		}
	};
	
	public int getStateCacheSize() {
		return stateCacheSize;
	}

	public void setStateCacheSize(int stateCacheSize) {
		this.stateCacheSize = stateCacheSize;
	}

	public Serializer getStateSerializer() {
		return stateSerializer;
	}

	public void setStateSerializer(Serializer stateSerializer) {
		this.stateSerializer = stateSerializer;
	}

	public TridentConfig(String tableName, String tupleRowKeyField) {
		super(tableName, tupleRowKeyField);
	}

	public TridentConfig(String tableName, String tupleRowKeyField,
			String tupleTimestampField, Map<String, Set<String>> columnFamilies) {
		super(tableName, tupleRowKeyField, tupleTimestampField, columnFamilies);
	}

	public TridentConfig(String tableName, String tupleRowKeyField,
			String tupleTimestampField) {
		super(tableName, tupleRowKeyField, tupleTimestampField);
	}
	

}
