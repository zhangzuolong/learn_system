package org.learn_system_tech.zookeeper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
* 
*    
* 项目名称：learn_system_tech   
* 类名称：InitConfReader   
* 类描述：   数据初始化
* 创建人：zhangzuolong   
* 创建时间：2016年7月20日 上午11:20:23      
* @version    
*
*/
public class InitConfReader {
    private String confFileUrl;
	public Map<String, String> getConfs(List<String> keys){
		Map<String, String> result = new HashMap<String, String>();
		Properties properties = new Properties();
		//从配置文件中读取配置信息,并用读取到的信息作为服务器启动的参数 
		try{
			properties.load(new FileReader(new File(confFileUrl)));
		} catch(FileNotFoundException e){
			e.printStackTrace();
		} catch(IOException e){
			e.printStackTrace();
		}		
		for(String key:keys){
			String value=(String) properties.get(key);
			result.put(key, value);
		}		
		return result;
	}

}
