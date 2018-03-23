package com.data.dataprocess.conf;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * spring 多线程处理配置
 * @author ztg 2018-3-21
 *
 */
@Configuration
public class AsyncTaskExecutePoolConfigurer implements AsyncConfigurer{
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Value("${executor.corePoolSize}")
	private int corePoolSize;
	@Value("${executor.maxPoolSize}")
	private int maxPoolSize;
	@Value("${executor.queueCapacity}")
	private int queueCapacity;
	@Value("${executor.keepAliveSeconds}")
	private int keepAliveSeconds;
	
	@Override
	public Executor getAsyncExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();  
        executor.setCorePoolSize(corePoolSize); 
        executor.setMaxPoolSize(maxPoolSize);    
        executor.setQueueCapacity(queueCapacity);    
        executor.setKeepAliveSeconds(keepAliveSeconds);    
        executor.setThreadNamePrefix("taskExecutor-");    
    
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());  
        executor.initialize();    
        return executor;
	}

	/**
	 * 线程异常处理
	 */
	@Override
	public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
		return new AsyncUncaughtExceptionHandler() {
			@Override
			public void handleUncaughtException(Throwable t, Method m,
					Object... obj) {
				logger.error("=========================="+t.getMessage()+"=======================", t);  
				logger.error("exception method:"+m.getName());  
			}
		};
	}

}
