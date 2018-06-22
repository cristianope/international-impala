package com.app99.international.configuration;

import org.postgresql.ds.PGPoolingDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class PostgresDataSourceConfiguration {

/*    @Value("${PG_PASS}")
    private String pass;

    @Value("{PG_DATABASE}")
    private String database;

    @Value("${PG_USER}")
    private String user;

    @Value("${PG_URL}")
    private String url;

    @Value("${PG_PORT}")
    private String port
*/
    @Bean(name = "pgPoolingDataSource", destroyMethod = "")
    public DataSource pgPoolingDataSource() {
        PGPoolingDataSource pgPoolingDataSource = new PGPoolingDataSource();
        pgPoolingDataSource.setServerName("52.23.190.227");
        pgPoolingDataSource.setDatabaseName("dw");
        pgPoolingDataSource.setUser("admindw");
        pgPoolingDataSource.setPassword("MX3Mpd9NsBAd22PU9BmAX7*AqxRgrbGZjGyGKNt5");
        pgPoolingDataSource.setMaxConnections(50);
        pgPoolingDataSource.setPortNumber(5439);
        return pgPoolingDataSource;
    }
}