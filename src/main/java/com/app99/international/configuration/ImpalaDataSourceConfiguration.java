package com.app99.international.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;


@Configuration
public class ImpalaDataSourceConfiguration {

    @Value("${IMPALA_URL}")
    private String impalaUrl;

    String DRIVER = "com.cloudera.impala.jdbc41.Driver";

    @Bean(name = "impalaPoolingDataSource", destroyMethod = "")
    public DriverManagerDataSource impalaPoolingDataSource() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName(DRIVER);
//      ds.setUrl("jdbc:impala://127.0.0.1:21051/new_app;UseNativeQuery=1;");
        ds.setUrl(impalaUrl);
        return ds;

    }
}
