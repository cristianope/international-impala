package com.app99.international.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class MySQLDataSourceConfiguration {

    @Bean(name = "myPoolingDataSource", destroyMethod = "")
    public DataSource myPoolingDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setPassword("#99taxis#");
        dataSource.setUrl("jdbc:mysql://db-cloudera.cpmjqhydxmyv.us-east-1.rds.amazonaws.com:3306/hivedb_k1iiujc5fnqaj3r1akldljulni");
        dataSource.setUsername("cloudera");
        return dataSource;
    }
}