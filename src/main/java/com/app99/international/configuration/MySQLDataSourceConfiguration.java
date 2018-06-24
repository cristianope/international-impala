package com.app99.international.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class MySQLDataSourceConfiguration {

    @Value("${MYSQL_PASS}")
    private String password;

    @Value("${MYSQL_USER}")
    private String user;

    @Value("${MYSQL_URL}")
    private String url;

    @Bean(name = "myPoolingDataSource", destroyMethod = "")
    public DataSource myPoolingDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
//        dataSource.setPassword("#99taxis#");
        dataSource.setPassword(password);
//        dataSource.setUrl("jdbc:mysql://db-cloudera.cpmjqhydxmyv.us-east-1.rds.amazonaws.com:3306/hivedb_k1iiujc5fnqaj3r1akldljulni");
//        dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/hivedb_k1iiujc5fnqaj3r1akldljulni");
        dataSource.setUrl(url);
//        dataSource.setUsername("cloudera");
        dataSource.setUsername(user);
        return dataSource;
    }
}