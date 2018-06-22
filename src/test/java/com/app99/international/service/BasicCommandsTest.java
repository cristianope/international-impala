package com.app99.international.service;

import com.app99.international.application.Application;
import com.app99.international.configuration.EnvironmentVariable;
import com.app99.international.service.impl.BasicCommands;
import com.app99.international.service.impl.ImpalaServiceImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Service;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration
public class BasicCommandsTest extends BasicCommands{

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicCommands.class);

    static{
        System.setProperty("QUEUE_ENDPOINT", "https://queue.amazonaws.com/492822123016/");
        System.setProperty("QUEUE_NAME", "international-impala");

        System.setProperty("MYSQL_PASS", "#99taxis#");
        System.setProperty("MYSQL_USER", "cloudera");
        System.setProperty("MYSQL_URL", "jdbc:mysql://127.0.0.1:3306/hivedb_k1iiujc5fnqaj3r1akldljulni");

        System.setProperty("PG_PASS", "MX3Mpd9NsBAd22PU9BmAX7*AqxRgrbGZjGyGKNt5");
        System.setProperty("PG_USER", "admindw");
        System.setProperty("PG_DATABASE", "dw");
        System.setProperty("PG_URL", "52.23.190.227");

        System.setProperty("IMPALA_URL", "jdbc:impala://127.0.0.1:21050");
        System.setProperty("JDBC_DRIVER", "com.cloudera.impala.jdbc41.Driver");
    }

    @Test
    public void getFieldsUseRedshift() throws Exception {
        String ddl = getFields("new_app", "dim_city", true);
        LOGGER.info("======================== " + ddl);
        assertTrue(ddl.contains("CAST(timezone AS bigint) , CAST(country_id AS bigint) , CAST(stat_date AS timestamp) , " +
                "CAST(stat_hour AS string) , CAST(current_stat_date AS string) , CAST(current_stat_hour AS string) , " +
                "CAST(city_id AS bigint) , CAST(district AS string) , CAST(name_zh AS string) , CAST(name_en AS string) , " +
                "CAST(car_prefix AS string) , CAST(center_lng AS decimal(38,6)) , CAST(center_lat AS decimal(38,6)) , " +
                "CAST(a_create_time AS string) , CAST(a_modify_time AS string) , CAST(is_use AS bigint) , CAST(country_name AS string) ," +
                " CAST(county_id AS bigint) , CAST(county_name AS string) , CAST(country_code AS string) "));
    }

    @Test
    public void getPartitionsOnlyFields() throws Exception {
        String ddl = getPartitions(",", "dim_city", "2018", "05", "05", "10", true );
        LOGGER.info("======================== " + ddl);
        assertTrue(ddl.contains("2018,05,05,10);
    }

    @Test
    public void getPartitions() throws Exception {
        String ddl = getPartitions(",", "dim_city", "2018", "05", "05", "10", false);
        LOGGER.info("======================== " + ddl);
        assertTrue(ddl.contains("year=2018,month=05,day=05,hour=10");
    }


    @Test
    public void hasPartitions() throws Exception {
        assertTrue(hasPartitions("dim_city"));
    }

    /*
    @Test
    public void externalTable() throws Exception {
        String ddl = createTable("redshift", "dim_city", true, existTable("new_app", "dim_city", true));
        assertTrue(ddl.contains("CREATE EXTERNAL TABLE redshift.dim_city IF NOT EXISTS (timezone bigint,country_id bigint,stat_date timestamp," +
                "stat_hour string,current_stat_date string,current_stat_hour string,city_id bigint,district string,name_zh string," +
                "name_en string,car_prefix string,center_lng decimal(38,6),center_lat decimal(38,6),a_create_time string,a_modify_time string," +
                "is_use bigint,country_name string,county_id bigint,county_name string,country_code string) PARTITIONED BY (year smallint," +
                "month tinyint,day tinyint,hour tinyint) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LOCATION " +
                "'s3a://99taxis-dw-international-online/hive-export/international/dim_city/' TBLPROPERTIES(\"skip.header.line.count\"=\"1\"); "));
    }

    @Test
    public void internalTable() throws Exception {
        String ddl = createTable("new_app", "dim_city", false, getCatalog().getFieldsDDL("redshift","dim_city"));
        assertTrue(ddl.contains("CREATE  TABLE new_app.dim_city IF NOT EXISTS (timezone bigint,country_id bigint,stat_date timestamp,stat_hour string," +
                "current_stat_date string,current_stat_hour string,city_id bigint,district string,name_zh string,name_en string,car_prefix string," +
                "center_lng decimal(38,6),center_lat decimal(38,6),a_create_time string,a_modify_time string,is_use bigint,country_name string," +
                "county_id bigint,county_name string,country_code string,day tinyint,hour tinyint) PARTITIONED BY (year smallint,month tinyint) STORED AS PARQUET;"));
    }

    @Test
    public void internalTableNotExistPartitions() throws Exception {
        String ddl = createTable("new_app", "dwm_driver_online_his", false, existTable("redshift", "dwm_driver_online_his", false, true));
        assertTrue(ddl.contains("CREATE  TABLE new_app.dwm_driver_online_his IF NOT EXISTS (timezone bigint,country_id bigint,stat_date string,stat_hour string," +
                "current_stat_date string,current_stat_hour string,product_id string,city_id bigint,driver_id bigint,first_listen_time string," +
                "last_listen_time string,total_driver_listen_time decimal(38,6),country_code string,year smallint,month smallint,day smallint,hour smallint) STORED AS PARQUET;"));
    }
*/

}

