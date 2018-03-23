package com.data.dataprocess.service;

import java.util.List;
import java.util.Map;


/**
 * 数据处理接口
 * @author ztg 2018-3-20
 *
 */
public interface IDataFileRead {
	
	/**
	 * 读取单个文件
	 * @param filePath 文件路径（包括文件名）
	 * @param regex 字段分隔符
	 * @param tableName 数据库对应表
	 * @param relationMap 文件中字段和数据表对应关系
	 * @param limitCount 分段处理条数
	 * @param validationMap 验证条件
	 * @param isValidation 是否开启根据数据表字段校验数据格式
	 * @param isIgnoreFirstLine 是否忽略首行
	 */
	public void readSingleFile2DB(String filePath,String regex,String tableName,Map<String,String> relationMap,int limitCount,Map<String,String> validationMap,boolean isValidation,boolean isIgnoreFirstLine);
	
	/**
	 * 根据目录读取文件
	 * @param fileDir 目录路径
	 * @param regex 字段分隔符
	 * @param tableName 数据库对应表
	 * @param relationMap 文件中字段和数据表对应关系
	 * @param limitCount 分段处理条数
	 * @param validationMap 验证条件
	 * @param isValidation 是否开启根据数据表字段校验数据格式
	 * @param isIgnoreFirstLine 是否忽略首行
	 */
	public void readFileByDir2DB(String fileDir,String regex,String tableName,Map<String,String> relationMap,int limitCount,Map<String,String> validationMap,boolean isValidation,boolean isIgnoreFirstLine);
	
	/**
	 * 校验数据
	 * @param list 校验数据集
	 * @param validationMap 验证条件
	 * @param info 日志信息
	 * @return 校验结果集
	 */
	public List<Map<String,Object>> validation(List<Map<String,Object>> list,Map<String,String> validationMap,String info);
}
