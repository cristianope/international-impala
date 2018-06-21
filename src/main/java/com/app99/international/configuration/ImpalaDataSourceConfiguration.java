package com.app99.international.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class ImpalaDataSourceConfiguration {

    @Bean(name = "impalaPoolingDataSource", destroyMethod = "")
    public DataSource impalaPoolingDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.cloudera.impala.jdbc41.Driver");
        dataSource.setUrl("jdbc:impala://IMPALAD_HOST:21050");
        return dataSource;
    }
}