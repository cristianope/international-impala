package com.app99.international.service;


import static org.junit.Assert.*;

import com.app99.international.application.Application;

import com.app99.international.model.OptionField;
import com.app99.international.service.impl.BasicCommands;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration
public class BasicCommandsTest extends BasicCommands{

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicCommands.class);
  
    private static final String DIM_CITY = "dim_city";
    private static final String NEW_APP = "new_app";
    private static final String REDSHIFT = "redshift";

    @Test
    public void getFieldsByOrderByPartitions() throws Exception {
        String ddl = getFieldsOrderByPartitions(REDSHIFT, DIM_CITY, true) ;
        assertTrue(ddl.contains("CAST(timezone AS bigint) , CAST(country_id AS bigint) , CAST(stat_date AS timestamp) ," +
                " CAST(stat_hour AS string) , CAST(current_stat_date AS string) , CAST(current_stat_hour AS string) ," +
                " CAST(city_id AS bigint) , CAST(district AS string) , CAST(name_zh AS string) , CAST(name_en AS string) ," +
                " CAST(car_prefix AS string) , CAST(center_lng AS decimal(38,6)) , CAST(center_lat AS decimal(38,6)) ," +
                " CAST(a_create_time AS string) , CAST(a_modify_time AS string) , CAST(is_use AS bigint) , CAST(country_name AS string) ," +
                " CAST(county_id AS bigint) , CAST(county_name AS string) , CAST(country_code AS string) , CAST(year AS smallint) ," +
                " CAST(month AS tinyint) , CAST(day AS tinyint) , CAST(hour AS tinyint) "));
    }


    @Test
    public void existTableRedshift() throws Exception {
        assertFalse(existTable(NEW_APP, DIM_CITY, true).isEmpty());
    }

    @Test
    public void exitTableMetaStore() throws Exception {
        assertFalse(existTable(REDSHIFT, DIM_CITY, false).isEmpty());
    }

    @Test
    public void existTableUseMetaStoreNotFound() throws Exception {
        assertTrue(existTable(NEW_APP, "table_not_exist", false).isEmpty());
    }

    @Test
    public void getFieldsUseRedshift() throws Exception {
        String ddl = getFields(NEW_APP, DIM_CITY, true);
        assertTrue(ddl.contains("CAST(timezone AS bigint) , CAST(country_id AS bigint) , CAST(stat_date AS timestamp) , " +
                "CAST(stat_hour AS string) , CAST(current_stat_date AS string) , CAST(current_stat_hour AS string) , " +
                "CAST(city_id AS bigint) , CAST(district AS string) , CAST(name_zh AS string) , CAST(name_en AS string) , " +
                "CAST(car_prefix AS string) , CAST(center_lng AS decimal(38,6)) , CAST(center_lat AS decimal(38,6)) , " +
                "CAST(a_create_time AS string) , CAST(a_modify_time AS string) , CAST(is_use AS bigint) , CAST(country_name AS string) ," +
                " CAST(county_id AS bigint) , CAST(county_name AS string) , CAST(country_code AS string) "));
    }

    @Test
    public void getPartitionsOnlyFields() throws Exception {
        String ddl = getPartitions(",", REDSHIFT,DIM_CITY, new String[] {"2018", "05", "05", "10"}, OptionField.ONLY_FIELDS );
        assertTrue(ddl.contains("2018,05,05,10"));
    }

    @Test
    public void getPartitionsOnlyValues() throws Exception {
        String ddl = getPartitions("/", REDSHIFT,DIM_CITY, new String[] {"2018", "05", "05", "10"}, OptionField.ONLY_VALUES);
        assertTrue(ddl.contains("year/month/day/hour"));
    }

    @Test
    public void getPartitionsFieds_Equal_Values() throws Exception {
        String ddl = getPartitions(",", REDSHIFT,DIM_CITY, new String[] {"2018", "05", "05", "10"}, OptionField.FIELD_EQUAL_VALUE);
        assertTrue(ddl.contains("year=2018,month=05,day=05,hour=10"));
    }


    @Test
    public void hasPartitions() throws Exception {
        assertTrue(hasPartitions(REDSHIFT, DIM_CITY));
        assertFalse(hasPartitions("backfill", DIM_CITY));
    }


    @Test
    public void externalTable() throws Exception {
        String ddl = createTable(REDSHIFT, DIM_CITY, true, existTable(NEW_APP, DIM_CITY, true));
        assertTrue(ddl.contains("CREATE EXTERNAL TABLE IF NOT EXISTS redshift.dim_city (timezone bigint,country_id bigint,stat_date timestamp," +
                "stat_hour string,current_stat_date string,current_stat_hour string,city_id bigint,district string,name_zh string," +
                "name_en string,car_prefix string,center_lng decimal(38,6),center_lat decimal(38,6),a_create_time string,a_modify_time string," +
                "is_use bigint,country_name string,county_id bigint,county_name string,country_code string) PARTITIONED BY (year smallint," +
                "month tinyint,day tinyint,hour tinyint) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LOCATION " +
                "'s3a://99taxis-dw-international-online/hive-export/international/dim_city/' TBLPROPERTIES(\"skip.header.line.count\"=\"1\"); "));
    }

    @Test
    public void internalTable() throws Exception {
        String ddl = createTable(NEW_APP, DIM_CITY, false, getCatalog().getFieldsDDL(REDSHIFT,DIM_CITY));
        assertTrue(ddl.contains("CREATE  TABLE IF NOT EXISTS new_app.dim_city (timezone bigint,country_id bigint,stat_date timestamp,stat_hour string," +
                "current_stat_date string,current_stat_hour string,city_id bigint,district string,name_zh string,name_en string,car_prefix string," +
                "center_lng decimal(38,6),center_lat decimal(38,6),a_create_time string,a_modify_time string,is_use bigint,country_name string," +
                "county_id bigint,county_name string,country_code string,year smallint,month tinyint,day tinyint,hour tinyint) STORED AS PARQUET;"));
    }

    @Test
    public void internalTableNotExistPartitions() throws Exception {
        String ddl = createTable(NEW_APP, "dwm_driver_online_his", false, existTable(REDSHIFT, "dwm_driver_online_his", false, true));
        assertTrue(ddl.contains("CREATE  TABLE IF NOT EXISTS new_app.dwm_driver_online_his (timezone bigint,country_id bigint,stat_date string," +
                "stat_hour string,current_stat_date string,current_stat_hour string,product_id string,city_id bigint,driver_id bigint,first_listen_time string," +
                "last_listen_time string,total_driver_listen_time decimal(38,6),country_code string,year smallint,month smallint,day smallint,hour smallint) " +
                "STORED AS PARQUET; "));
    }

    @Test
    public void internalTablePartitions() throws Exception {
        String ddl = createTable("redshift", "dwd_order_broadcast_hi", false, existTable(REDSHIFT, "dwd_order_broadcast_hi", false));
        assertTrue(ddl.contains("CREATE  TABLE IF NOT EXISTS redshift.dwd_order_broadcast_hi (timezone bigint,country_id bigint,stat_date string,stat_hour string," +
                "current_stat_date string,current_stat_hour string,order_id bigint,product_id bigint,city_id bigint,district_id bigint,order_timelines bigint," +
                "passenger_id bigint,driver_id bigint,listen_model bigint,listen_dis decimal(38,6),car_level bigint,status bigint,join_model bigint,bonus decimal(38,6)," +
                "dynamic_price decimal(38,6),dynamic_times bigint,eta bigint,walk_eta bigint,broad_num bigint,traffic_type bigint,lumian_dis decimal(38,6)," +
                "order_wait_time bigint,is_assign_driver bigint,booking_assign bigint,broad_type bigint,is_online bigint,start_dest_distance decimal(38,6),distance decimal(38,6)," +
                "broad_time string,day smallint,hour smallint) PARTITIONED BY (year smallint,month tinyint) STORED AS PARQUET; "));
    }

}

