package com.app99.international.configuration;

import com.amazon.redshift.common.PGCommonDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class RedshiftDataSourceConfiguration {

    @Value("${PG_PASS}")
    private String pass;

    @Value("${PG_USER}")
    private String user;

    @Value("${PG_URL}")
    private String url;

    @Bean(name = "pgPoolingDataSource", destroyMethod = "")
    public DataSource pgPoolingDataSource() {
        PGCommonDataSource pgPoolingDataSource = new com.amazon.redshift.jdbc41.DataSource();
        pgPoolingDataSource.setURL(url);
//        pgPoolingDataSource.setURL("jdbc:redshift://dw.cthopyfgalif.us-east-1.redshift.amazonaws.com:5439/dw");
        pgPoolingDataSource.setUserID(user);
//        pgPoolingDataSource.setUserID("admindw");
//        pgPoolingDataSource.setPassword("MX3Mpd9NsBAd22PU9BmAX7*AqxRgrbGZjGyGKNt5");
        pgPoolingDataSource.setPassword(pass);
        return pgPoolingDataSource;
    }
}