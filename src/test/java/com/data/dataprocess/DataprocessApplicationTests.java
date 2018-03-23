package com.data.dataprocess;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.data.dataprocess.dao.CommonOper;
import com.data.dataprocess.service.IDataFileRead;

/**
 * 测试类，用于文件生成，文件读取
 * @author ztg 2018-3-20
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class DataprocessApplicationTests {

	@Value("${readFilePath}") 
	private String filePath;
	
	@Value("${limitCount}") 
	private int limitCount;
	
	@Autowired
	private CommonOper tableInfo;
	
	@Autowired
	private IDataFileRead dataFileRead;
	
	/**
	 * 初始化文件,生成测试文件，此处硬编码 TODO
	 */
	@Test
	public void contextLoads() {
		long startTime = System.currentTimeMillis();
		// 创建10天的文件 TODO
		int days = 10;
		// 每个文件数据量 TODO
		int dataCount =10000;
		
		File file = new File(filePath);
		if(!file.isDirectory()){
			file.mkdirs();
		}
		
		for (int i = 0; i < days; i++) {
			// 时钟操作
			Calendar cal=Calendar.getInstance();
			cal.add(Calendar.DATE,-i);
			Date time=cal.getTime();
			//格式化时间
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			String dateNowStr = sdf.format(time);
			
			// 创建文件
			File newFile= new File(filePath+File.separator+dateNowStr+".csv");
			// 先删除
			if(newFile.isFile()){
				newFile.delete();
				
			}
			// 再创建
			try {
				newFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
			FileOutputStream outputStream = null;
			// 每个文件插入数据
			try {
				for (int j = 0; j < dataCount; j++) {
					outputStream = new FileOutputStream(newFile, true);
					FileChannel fileChannel = outputStream.getChannel();
					// TODO 写入数据验证
					byte[] bytes = "00000x11,0.01123,1.02213,-1.01001\r\n".getBytes();
					ByteBuffer bbf = ByteBuffer.allocate(bytes.length);  
					bbf.put(bytes);
	                bbf.flip(); 
					fileChannel.write(bbf);
					
					fileChannel.close();
					outputStream.flush();
					outputStream.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				try {
					if(outputStream!=null){
						outputStream.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("生成测试文件耗时："+(System.currentTimeMillis() - startTime));
	}
	
	/**
	 * 测试读取文件到数据库 TODO
	 * 注意数据库采用了主键约束，多次读统一文件会报错
	 */
	@Test
	public void readFile2DB(){
		long startTime = System.currentTimeMillis();
		
		// 输入输出对应关系 TODO
		Map<String,String> relationMap = new HashMap<String, String>();
		// 行号作为字段输入到数据库对应字段item_id
		relationMap.put("LINENUM", "item_id");
		// 文件名作为字段输入到数据库对应字段trading_date
		relationMap.put("FILENAME", "trading_date");
		// 第0列作为数据库stock_code字段
		relationMap.put("0", "stock_code");
		// 第2列作为数据库item_value字段
		relationMap.put("2", "item_value");
		// 验证正则表达式 key代表需要验证的字段，value代表正则表达式（简单实现）
		Map<String,String> validationMap = new HashMap<String, String>();
		// 整数验证正则"^-?\\d+$"  浮点数："^(-?\\d+)(\\.\\d+)?$" 日期："^\\d{4}-\\d{1,2}-\\d{1,2}$"  字符串长度限制："^\\w{0,10}$"
		validationMap.put("item_id", "^-?\\d+$");
		validationMap.put("trading_date", "^\\d{4}-\\d{1,2}-\\d{1,2}$");
		validationMap.put("stock_code", "^\\w{0,10}$");
		validationMap.put("item_value", "^(-?\\d+)(\\.\\d+)?$");
		
		// 读取单个文件到数据库
		// dataFileRead.readSingleFile2DB(filePath+File.separator+"2018-03-23.csv", ",", "time_series_data", relationMap, limitCount, validationMap, true, false);
		// 读取文件目录下文件数据库
		dataFileRead.readFileByDir2DB(filePath, ",", "time_series_data", relationMap, limitCount, validationMap, true, false);
		System.out.println("读取文件耗时："+(System.currentTimeMillis() - startTime));
	}
	
	/**
	 * 测试类，简单文件读取验证
	 * @throws Exception
	 */
	@Test
	public void readFile() throws Exception{
		long startTime = System.currentTimeMillis();
		LineNumberReader lineNumberReader =null;
		FileReader reader = null;
		String readFile = filePath+File.separator+"2018-03-12.csv";
		try {
			File file =new File(readFile);
			reader = new FileReader(file);
			lineNumberReader = new LineNumberReader(reader);
			int count = 0;
			String str = "";
			while((str=lineNumberReader.readLine())!=null){
				count++;
				System.out.println(str);
			}
			
			System.out.println(count);
			lineNumberReader.close();
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(lineNumberReader!=null){
				lineNumberReader.close();
			}
			if(reader!=null){
				reader.close();
			}
		}
		
		System.out.println("读取文件到数据库耗时："+(System.currentTimeMillis() - startTime));
	}

	/**
	 * 测试获取表信息
	 */
	@Test
	public void testGetTableInfo(){
		List<Map<String, String>> list =tableInfo.getColumsInfo("time_series_data");
		
		for (int i = 0; i < list.size(); i++) {
			Map<String, String> map = list.get(i);
			for (String key:map.keySet()) {
				String value = map.get(key);
				System.out.println("key:"+key+",value:"+value);
			}
		}
	}
}
