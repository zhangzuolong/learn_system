package org.learn_system_tech.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
*    
* 项目名称：learn_system_tech   
* 类名称：UPPER   
* 类描述： pig 大小写UDF(user define functions)--用户自定义函数  
* 创建人：zhangzuolong   
* 创建时间：2016年7月19日 上午9:49:55      
* @version    
*
*/
public class UPPER  extends EvalFunc<String>{

	@Override
	public String exec(Tuple input) throws IOException {
		if(input == null || input.size() == 0){
			return null;
		}
		try{
			String str = String.valueOf(input.get(0));
			return str.toUpperCase();
		}catch(Exception e){
			throw new IOException("Caught excetion"+e.getMessage());
		}
	}

}
