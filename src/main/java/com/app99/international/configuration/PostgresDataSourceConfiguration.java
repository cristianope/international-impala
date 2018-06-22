package com.app99.international.configuration;

import org.postgresql.ds.PGPoolingDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class PostgresDataSourceConfiguration {

    @Value("${PG_PASS}")
    private String password;

    @Value("{PG_DATABASE}")
    private String database;

    @Value("${PG_USER}")
    private String user;

    @Value("${PG_URL}")
    private String url;

    @Bean(name = "pgPoolingDataSource", destroyMethod = "")
    public DataSource pgPoolingDataSource() {
        PGPoolingDataSource pgPoolingDataSource = new PGPoolingDataSource();
        pgPoolingDataSource.setServerName(url);
        pgPoolingDataSource.setDatabaseName(database);
        pgPoolingDataSource.setUser(user);
        pgPoolingDataSource.setPassword(password);
        pgPoolingDataSource.setMaxConnections(50);
        return pgPoolingDataSource;
    }
}