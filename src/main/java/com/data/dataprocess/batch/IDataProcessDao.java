package com.data.dataprocess.batch;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 批量多线程插入数据库
 * @author ztg 2018-3-22
 *
 */
public interface IDataProcessDao {
	/**
	 * 批量插入数据
	 * @param sqlList sql链表
	 * @param info 附加日志信息
	 * @param countDownLatch 线程计数器
	 */
	public void batchInsert(List<String> sqlList,String info,CountDownLatch countDownLatch);
}
