package com.data.dataprocess.batch.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.mybatis.spring.SqlSessionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Repository;

import com.data.dataprocess.batch.IDataProcessDao;

@Repository
public class DataProcessDao implements IDataProcessDao{
	Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Autowired
	private SqlSessionTemplate sqlSessionTemplate;
	
	@Override
	@Async
	public void batchInsert(List<String> sqlList,String info,CountDownLatch countDownLatch){
		logger.info("---------->当前线程："+Thread.currentThread().getName()+"--->处理数据："+info);
		StringBuilder builder = new StringBuilder(info);

		// 关闭批量提交
		SqlSession session = null;
		Connection connection = null;
		Statement statement = null;
		// 优先尝试批量处理
		try {
			session = sqlSessionTemplate.getSqlSessionFactory().openSession(ExecutorType.BATCH,false);
			connection = session.getConnection();
			// 关闭自动提交事务
			connection.setAutoCommit(false);
			statement= connection.createStatement();
			for (int i = 0; i < sqlList.size(); i++) {
				statement.addBatch(sqlList.get(i));
				// 批量提交事务
				if( i==(sqlList.size()-1)){
					statement.executeBatch();
					connection.commit();
					statement.close();
					connection.close();
					session.close();
				}
			}
		} catch (Exception e) {
			// 如果发生异常 回滚并打印日志
			logger.error(builder.append("-->批量提交错误{}").toString(), e);
			try {
				connection.rollback();
				statement.clearBatch();
			} catch (SQLException e1) {
				logger.error("清除statement batch异常！");
			}
				// 单条记录提交
				for (int i = 0; i < sqlList.size(); i++) {
					try {
						statement.execute(sqlList.get(i));
						connection.commit();
						} catch (Exception e2) {
							// 记录错误信息并回滚事务
							logger.error(builder.append("-->再次尝试单条记录提交错误，错误信息定位：")
									.append(i).append("-->").append(sqlList.get(i)).append("-->{}").toString(),e2);
							try {
								if(connection!=null){
									connection.rollback();
								}
							} catch (SQLException e1) {
								logger.error("事务回滚异常！");
							}
						}
				}
		}finally{
			if(statement!=null){
				try {
					statement.close();
				} catch (SQLException e) {
					logger.error("关闭statement错误！",e);
				}
			}
			if(connection!=null){
				try {
					connection.close();
				} catch (SQLException e) {
					logger.error("关闭connection错误！",e);
				}
			}
			if(session!=null){
				session.close();
			}
			countDownLatch.countDown();
			logger.info("子线程执行完毕！");
		}
	}

}
