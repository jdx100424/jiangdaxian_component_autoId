package com.jiangdaxian.autoid.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.jiangdaxian.autoid.api.TableAutoIdApi;
import com.jiangdaxian.autoid.model.TableAutoIdModel;

@Component
public class AutoIdUtil {
	private static final Logger LOG = LoggerFactory.getLogger(AutoIdUtil.class);

	// 全局项目和表的REDIS锁，参数1，项目名，参数2，表名
	private static final String PROJECT_TABLE_LOCAK_LOCK = "PROJECT_TABLE_LOCAL_LOCK:%s:%s";

	// 本地保存的一批ID号
	private static final Map<String, LinkedList<Long>> AUTOID_LOCAL = new HashMap<String, LinkedList<Long>>();
	
	@Autowired
	private TableAutoIdApi tableAutoIdApi;

	public Long getNextId(String projectName, String tableName) throws Exception {
		String str = String.format(PROJECT_TABLE_LOCAK_LOCK, projectName, projectName);
		if (AUTOID_LOCAL.get(str) == null) {
			//系统刚启动的一下
			synchronized (this) {
				if(AUTOID_LOCAL.get(str)==null) {
					AUTOID_LOCAL.put(str, new LinkedList<Long>());
				}
			}
		}
		
		Long result = null;
		try {
			result = AUTOID_LOCAL.get(str).pop();
		}catch(NoSuchElementException e) {
			
		}
		if(result==null) {
			//如ID已全部用完，重新去获取
			synchronized (AUTOID_LOCAL.get(str)) {
				if(AUTOID_LOCAL.get(str).isEmpty()) {
					//LOG.info("thread name:{},projectName:{},tableName:{}",Thread.currentThread().getName(),projectName,tableName);
					try {
						TableAutoIdModel tableAutoIdModel = tableAutoIdApi.getProjectTableId(projectName, tableName);
						if(tableAutoIdModel!=null) {
							Long start = tableAutoIdModel.getStartId();
							Long end = tableAutoIdModel.getEndId();
							for(long l=end;l>=start;l--) {
								AUTOID_LOCAL.get(str).push(l);
							}
						}
					}catch(Exception e) {
						LOG.error(e.getMessage(),e);
						throw new Exception("pop id exception");
					}
				}
			}
			
			try {
				result = AUTOID_LOCAL.get(str).pop();
			}catch(NoSuchElementException e) {
				
			}
			
			if(result==null) {
				throw new Exception("pop 2 id fail");
			}else {
				return result;
			}
		}else {
			return result;
		}
	}
}
