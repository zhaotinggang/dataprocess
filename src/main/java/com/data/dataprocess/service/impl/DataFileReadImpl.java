package com.data.dataprocess.service.impl;

import java.io.File;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.mockito.internal.util.io.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.druid.util.StringUtils;
import com.data.dataprocess.batch.IDataProcessDao;
import com.data.dataprocess.contants.CommonContants;
import com.data.dataprocess.service.IDataFileRead;

/**
 * 数据文件读取实现类
 * 暂未完善功能：
 * 1、读取过的文件如果再次执行会再次读取；
 * 2、暂时不支持文本中的多列组合计算 存到 指定数据库表的指定字段；
 * 3、暂不支持读到重复数据更新到数据库，目前异常日志记录处理；
 * @author ztg 2018-3-20
 *
 */
@Service
public class DataFileReadImpl implements IDataFileRead{
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Autowired
	private IDataProcessDao iDataProcessDao;

	@Override
	public List<Map<String, Object>> validation(List<Map<String, Object>> list,Map<String,String> validationMap,String info) {
		List<Map<String, Object>> returnList = new ArrayList<Map<String, Object>>();
		for (int i = 0; i < list.size(); i++) {
			Map<String, Object> map = list.get(i);
			boolean flag = true;
			StringBuilder builder = new StringBuilder("字段不符合格式-->"); 
			for (String key:map.keySet()) {
				// 正则匹配
				Object value = map.get(key);
				String valString = "";
				String valregex = validationMap.get(key);
				Pattern pattern = Pattern.compile(valregex);
				if(value!=null){
					valString = value.toString();
					Matcher matcherRs = pattern.matcher(valString);
					if(!matcherRs.matches()){
						builder.append(value);
						builder.append(",");
						flag = false;
					}
				}
			}
			if(flag){
				returnList.add(map);
			}else{
				builder.append("校验不通过--->");
				builder.append(info);
				builder.append("--->行号：");
				builder.append(i);
				logger.warn(builder.toString());
			}
		}
		return returnList;
	}

	@Override
	public void readSingleFile2DB(String filePath, String regex,
			String tableName, Map<String, String> relationMap, int limitCount,
			Map<String,String> validationMap,boolean isValidation, boolean isIgnoreFirstLine) {
		File file = new File(filePath);
		if(file.isFile() && file.canRead()){
			LineNumberReader  lineNumberReader = null;
			LineNumberReader  countLineNumberReader = null;
			FileReader countFileReader = null;
			FileReader fileReader = null;
			try {
				// 优先统计数据流
				countFileReader = new FileReader(file);
				countLineNumberReader =  new LineNumberReader(countFileReader);
				
				countLineNumberReader.skip(Long.MAX_VALUE);
				int lineCount = countLineNumberReader.getLineNumber();
				// 关闭统计流
				IOUtil.close(countLineNumberReader);
				IOUtil.close(countFileReader);
				
				// 多线程内存屏蔽
				// 计算处理次数
				int commitCount = 0;
				if(lineCount%limitCount == 0 ){
					commitCount = lineCount/limitCount;
				}else{
					commitCount = lineCount/limitCount+1;
				}
				CountDownLatch countDownLatch = new CountDownLatch(commitCount);
				
				// 获取输入流
				fileReader = new FileReader(file);
				lineNumberReader =  new LineNumberReader(fileReader);
				
				
				// 行读取
				String str = "";
				// 行计算器
				int count = 0;
				List<Map<String, Object>> list = null;
				while((str = lineNumberReader.readLine())!=null){
					// 
					// 是否忽略第一行，可能是标题,或者空行也忽略
					if((isIgnoreFirstLine && count==0)){
						count++;
						logger.info("忽略了首行");
						continue;
					}
					if("".equals(str)){
						logger.error("存在空行，行数"+count);
						continue;
					}
					if(list==null){
						list = new ArrayList<Map<String,Object>>();
					}
					// 分割为数组
					String[] array = str.split(regex);
					Map<String, Object> newMap = new HashMap<String, Object>();
					// 根据传入的匹配map获取文件中需要存到数据库的字段
					for (String fileKey : relationMap.keySet()) {
						// 数字索引优先处理，特殊处理uuid、文件名、使用文件中行作为字段输出  TODO 此处可扩展多种形式
						if(StringUtils.isNumber(fileKey)){
							int location = Integer.parseInt(fileKey);
							if(location < array.length){
								String field = array[location];
								newMap.put(relationMap.get(fileKey), field);
							}
						}else if(CommonContants.FILENAME.equals(fileKey)){
							String fileName = file.getName();
							fileName = fileName.substring(0, fileName.indexOf("."));
							newMap.put(relationMap.get(fileKey),fileName);
						}else if(CommonContants.LINENUM.equals(fileKey)){
							newMap.put(relationMap.get(fileKey), count);
						}else if(CommonContants.UUID.equals(fileKey)){
							newMap.put(relationMap.get(fileKey), UUID.randomUUID());
						}
					}
					list.add(newMap);
					// 满足处理条数处理条件或者最后一行，处理一批数据
					int nowLineNum = lineNumberReader.getLineNumber();
					if((count%limitCount == 0 && count!=0) || nowLineNum==lineCount){
						int startLine = 0;
						if(nowLineNum-limitCount>0){
							startLine = nowLineNum-limitCount;
						}
						String info = file.getName()+"-->"+startLine+"-->"+nowLineNum;
						// 验证数据
						if(isValidation){
							list = validation(list,validationMap,info);
						}
						// 动态sql拼装
						List<String> sqlList = daynicSql(list,relationMap,tableName);
						logger.info("执行sql前："+info);
						// 执行sql
						iDataProcessDao.batchInsert(sqlList, info,countDownLatch);
						// 结尾重置list
						list = new ArrayList<Map<String,Object>>();
					}
					count++;
				}
				// 主线程放开内存屏蔽
				countDownLatch.await();
				logger.info("主线程执行！");
				
				// 结束后将读完的文件写入记录 （此处暂未考虑断点续读问题） TODO 暂时未处理
				
				// 关闭流
				IOUtil.close(lineNumberReader);
				IOUtil.close(fileReader);
			} catch (Exception e) {
				logger.error("读取文件到数据库报错-->{}",e);
			}finally{
				// 异常关闭流
				IOUtil.close(countLineNumberReader);
				IOUtil.close(lineNumberReader);
				IOUtil.close(countFileReader);
				IOUtil.close(fileReader);
			}
		}else{
			logger.error("文件路径出错或不可读："+filePath);
		}
	}
	
	/**
	 * 入库操作
	 * @param list
	 * @param relationMap
	 * @param tableName
	 */
	private List<String> daynicSql(List<Map<String, Object>> list,Map<String, String> relationMap,String tableName){
		List<String> sqlList = new ArrayList<String>();
		// 组装模板
		StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
		sqlBuilder.append(tableName);
		sqlBuilder.append(" ( ");
		relationMap.entrySet();
		// sql字段拼装
		for (String key:relationMap.keySet()) {
			sqlBuilder.append(relationMap.get(key));
			sqlBuilder.append(",");
		}
		if(sqlBuilder.indexOf(",")>0){
			sqlBuilder.delete(sqlBuilder.length()-1, sqlBuilder.length());
		}
		sqlBuilder.append(" ) values ( ");
		// sql参数拼装
		for (String key:relationMap.keySet()) {
			sqlBuilder.append("#");
			sqlBuilder.append(relationMap.get(key));
			sqlBuilder.append("#");
			sqlBuilder.append(",");
		}
		if(sqlBuilder.indexOf(",")>0){
			sqlBuilder.delete(sqlBuilder.length()-1, sqlBuilder.length());
		}
		sqlBuilder.append(" ) ");
		for (int i = 0; i < list.size(); i++) {
			Map<String, Object> dataMap = list.get(i);
			String sql = sqlBuilder.toString();
			for (String dataKey : dataMap.keySet()) {
				sql = sql.replaceAll("#"+dataKey+"#", "'" + dataMap.get(dataKey) +"'");
			}
			sqlList.add(sql);
		}
		return sqlList;
	}

	@Override
	public void readFileByDir2DB(String fileDir, String regex,
			String tableName, Map<String, String> relationMap,int limitCount,
			Map<String,String> validationMap,boolean isValidation, boolean isIgnoreFirstLine) {
		File file = new File(fileDir);
		// 循环遍历获取目录下的文档（目前暂时只支持一级目录）
		if(file.isDirectory()){
			String fileAbsPath = file.getAbsolutePath();
			String[] files = file.list();
			for (String fileName : files) {
				String signaleFileName = fileAbsPath + File.separator + fileName;
				readSingleFile2DB(signaleFileName, regex, tableName, relationMap, limitCount,validationMap,isValidation, isIgnoreFirstLine);
			}
		}else{
			logger.error("文件目录出错："+fileDir);
		}
	}
}
