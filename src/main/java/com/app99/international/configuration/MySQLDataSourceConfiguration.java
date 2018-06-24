package com.app99.international.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class MySQLDataSourceConfiguration {

    @Value("${MYSQL_PASS}")
    private String myPass;

    @Value("${MYSQL_USER}")
    private String myUser;

    @Value("${MYSQL_URL}")
    private String myUrl;

    @Bean(name = "myPoolingDataSource", destroyMethod = "")
    public DataSource myPoolingDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
//        dataSource.setPassword("#99taxis#");
        dataSource.setPassword(myPass);
//        dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/hivedb_k1iiujc5fnqaj3r1akldljulni");
        dataSource.setUrl(myUrl);
        dataSource.setUsername(myUser);
        return dataSource;
    }
}