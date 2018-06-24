package com.app99.international.service;


import com.app99.international.application.Application;
import com.app99.international.service.impl.BasicCommands;
import com.app99.international.service.impl.ImpalaServiceImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration
public class ImpalaServiceImplTest extends ImpalaServiceImpl {

    @Autowired
    private ImpalaServiceImpl service;

    private static final String DIM_CITY = "dim_city";
    private static final String ODS_LOG = "ods_log_pbs_starfall_audit";
    private static final String NEW_APP = "new_app";
    private static final String REDSHIFT = "redshift";
    private static final String APP_COUPON = "app_coupon_base_bra_cube_d";
    private static final String DWD_ORDER_PAY =  "dwd_order_pay_success_hi";




    private static final Logger LOGGER = LoggerFactory.getLogger(BasicCommands.class);


    @Test
    public void createInsertCommand() throws Exception {
        String ddl = createInsertCommand(NEW_APP, DIM_CITY, getPartitionsImpalaNewApp(DIM_CITY), true);
        assertTrue(ddl.contains("SET compression_codec=snappy; SET parquet_file_size=256mb; INSERT INTO new_app.dim_city PARTITION () "));

        ddl = createInsertCommand(NEW_APP, DIM_CITY, getPartitionsImpalaNewApp(DIM_CITY), false);
        assertTrue(ddl.contains("INSERT INTO new_app.dim_city"));
    }

    @Test
    public void createSelectCommand() throws Exception {
        String ddl = createSelectCommand(NEW_APP, REDSHIFT, DIM_CITY, getPartitionsImpalaRedShift(DIM_CITY, new String[]{"2018", "05", "05", "10"}), true);
        assertTrue(ddl.contains("SELECT DISTINCT  CAST(timezone AS bigint) , CAST(country_id AS bigint) , " +
                "CAST(stat_date AS timestamp) , CAST(stat_hour AS string) , CAST(current_stat_date AS timestamp) , " +
                "CAST(current_stat_hour AS string) , CAST(city_id AS bigint) , CAST(district AS string) , CAST(name_zh AS string) , " +
                "CAST(name_en AS string) , CAST(car_prefix AS string) , CAST(center_lng AS decimal(38,6)) , CAST(center_lat AS decimal(38,6)) , " +
                "CAST(a_create_time AS timestamp) , CAST(a_modify_time AS timestamp) , CAST(is_use AS bigint) , CAST(country_name AS string) , " +
                "CAST(county_id AS bigint) , CAST(county_name AS string) , CAST(country_code AS string) , CAST(year AS smallint) , CAST(month AS tinyint) ," +
                " CAST(day AS tinyint) , CAST(hour AS tinyint)  FROM redshift.dim_city WHERE year=2018 AND month=05 AND day=05 AND hour=10; "));

    }

    @Test
    public void createSelectCommandPartitions() throws Exception {
        String ddl = createSelectCommand(NEW_APP, REDSHIFT, ODS_LOG, getPartitionsImpalaRedShift(DIM_CITY, new String[]{"2018", "05", "05", "10"}), true);
        assertTrue(ddl.contains("SELECT  CAST(prefix_key AS string) , CAST(param AS string) , CAST(day AS tinyint) , CAST(hour AS tinyint) ,  " +
                "CAST(year AS smallint) , CAST(month AS tinyint)  FROM redshift.ods_log_pbs_starfall_audit WHERE year=2018 AND month=05 AND day=05 AND hour=10;"));

    }



    @Test
    public void AddPartitionsS3DayPartition() throws Exception {
        String ddl = service.AddPartitionsS3(REDSHIFT, APP_COUPON, new String[] {"2018", "05", "05", null});
        assertTrue(ddl.contains("ALTER TABLE redshift.app_coupon_base_bra_cube_d ADD PARTITION(year=2018,month=05,day=05) " +
                "LOCATION 's3a://99taxis-dw-international-online/hive-export/international/app_coupon_base_bra_cube_d/2018/05/05/';" +
                " COMPUTE INCREMENTAL STATS redshift.app_coupon_base_bra_cube_d PARTITION(year=2018,month=05,day=05);"));
    }

    @Test
    public void AddPartitionsS3() throws Exception {
        String ddl = service.AddPartitionsS3(REDSHIFT, DWD_ORDER_PAY , new String[]{"2018", "05", "05", "10"});
        assertTrue(ddl.contains("ALTER TABLE redshift.dwd_order_pay_success_hi ADD PARTITION(year=2018,month=05,day=05,hour=10) " +
                "LOCATION 's3a://99taxis-dw-international-online/hive-export/international/dwd_order_pay_success_hi/2018/05/05/10/';" +
                " COMPUTE INCREMENTAL STATS redshift.dwd_order_pay_success_hi PARTITION(year=2018,month=05,day=05,hour=10);"));
    }



    @Test
    public void backFillTable() throws Exception {
        String ddl = service.backfillTable(DWD_ORDER_PAY);
        assertTrue(ddl.contains("CREATE  TABLE IF NOT EXISTS new_app.dwd_order_pay_success_hi (timezone bigint,country_id bigint," +
                "stat_date timestamp,stat_hour string,current_stat_date string,current_stat_hour string,order_id bigint,city_id bigint," +
                "product_id bigint,passenger_id bigint,driver_id bigint,payable_cost decimal(38,6),actual_cost decimal(38,6)," +
                "is_use_coupon bigint,coupon_spend decimal(38,6),pay_info string,country_code string,day smallint,hour smallint) " +
                "PARTITIONED BY (year smallint,month tinyint) STORED AS PARQUET; SET compression_codec=snappy; SET parquet_file_size=256mb;" +
                " INSERT INTO new_app.dwd_order_pay_success_hi SELECT  CAST(timezone AS bigint) , CAST(country_id AS bigint) ," +
                " CAST(stat_date AS timestamp) , CAST(stat_hour AS string) , CAST(current_stat_date AS timestamp) ," +
                " CAST(current_stat_hour AS string) , CAST(order_id AS bigint) , CAST(city_id AS bigint) , CAST(product_id AS bigint) ," +
                " CAST(passenger_id AS bigint) , CAST(driver_id AS bigint) , CAST(payable_cost AS decimal(38,6)) , CAST(actual_cost AS decimal(38,6)) ," +
                " CAST(is_use_coupon AS bigint) , CAST(coupon_spend AS decimal(38,6)) , CAST(pay_info AS string) , CAST(country_code AS string) ," +
                " CAST(day AS tinyint) , CAST(hour AS tinyint) ,  CAST(year AS smallint) , CAST(month AS tinyint)  FROM backfill.dwd_order_pay_success_hi ; "));
    }


    @Test
    public void prepareCommand() throws Exception {
        String ddl = service.prepareCommand(REDSHIFT, NEW_APP, "dwd_order_pay_success_hi", "2018", "05", "05", "10" );
        assertTrue(ddl.contains("ALTER TABLE new_app.dwd_order_pay_success_hi DROP PARTITION (year=2018,month=05,day=05,hour=10); " +
                "SET compression_codec=snappy; SET parquet_file_size=256mb; INSERT INTO new_app.dwd_order_pay_success_hi PARTITION" +
                " (year,month) SELECT  CAST(timezone AS bigint) , CAST(country_id AS bigint) , CAST(stat_date AS timestamp) , " +
                "CAST(stat_hour AS string) , CAST(current_stat_date AS timestamp) , CAST(current_stat_hour AS string) , CAST(order_id AS bigint) ," +
                " CAST(city_id AS bigint) , CAST(product_id AS bigint) , CAST(passenger_id AS bigint) , CAST(driver_id AS bigint) , " +
                "CAST(payable_cost AS decimal(38,6)) , CAST(actual_cost AS decimal(38,6)) , CAST(is_use_coupon AS bigint) , CAST(coupon_spend AS decimal(38,6)) " +
                ", CAST(pay_info AS string) , CAST(country_code AS string) , CAST(day AS tinyint) , CAST(hour AS tinyint) ,  CAST(year AS smallint) ," +
                " CAST(month AS tinyint)  FROM redshift.dwd_order_pay_success_hi WHERE year=2018 AND month=05 AND day=05 AND hour=10; " +
                "COMPUTE INCREMENTAL STATS new_app.dwd_order_pay_success_hi PARTITION(year=2018,month=05,day=05,hour=10); "));
    }


    @Test
    public void executeCommandDDL() throws Exception {
       assertFalse(service.executeQuery("SET compression_codec=snappy; SET parquet_file_size=256mb; "));
    }

    @Test
    public void executeCommandDQL() throws Exception {
        assertTrue(service.executeQuery("SELECT 1;"));
    }
}
