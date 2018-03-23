package com.data.dataprocess.dao;

import java.util.List;
import java.util.Map;

/**
 * 公共信息dao层接口
 * @author ztg 2018-3-21
 *
 */
public interface CommonOper {
	/**
	 * 根据表名获取列信息
	 * @param tableName
	 * @return
	 */
	public List<Map<String,String>> getColumsInfo(String tableName);
}
