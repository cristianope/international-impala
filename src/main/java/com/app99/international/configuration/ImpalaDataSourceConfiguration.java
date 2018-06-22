package com.app99.international.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class ImpalaDataSourceConfiguration {


    @Value("${IMPALA_URL}")
    private String url;

    @Bean(name = "impalaPoolingDataSource", destroyMethod = "")
    public DataSource impalaPoolingDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.cloudera.impala.jdbc41.Driver");
//        dataSource.setUrl("jdbc:impala://IMPALAD_HOST:21050");
        dataSource.setUrl(url);
        return dataSource;
    }
}