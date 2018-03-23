package com.data.dataprocess;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@MapperScan("com.data.dataprocess.dao")
@EnableAsync
public class DataprocessApplication {
	
	public static void main(String[] args) {
		SpringApplication.run(DataprocessApplication.class, args);
	}
}
