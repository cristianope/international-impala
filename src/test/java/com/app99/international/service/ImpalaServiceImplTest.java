package com.app99.international.service;

import com.app99.international.application.Application;
import com.app99.international.configuration.EnvironmentVariable;
import com.app99.international.dao.impl.HiveMetastoreDAOImpl;
import com.app99.international.model.Field;
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

import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration
public class ImpalaServiceImplTest extends EnvironmentVariable {

    @Autowired
    private ImpalaServiceImpl service;

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicCommands.class);

    @Test
    public void AddPartitionsS3() throws Exception {
        String ddl = service.AddPartitionsS3("redshift", "dwd_order_pay_success_hi", "2018", "05", "05", "10");
        LOGGER.info("========== DDL " + ddl);
        assertTrue(ddl.contains("ALTER TABLE redshift.dwd_order_pay_success_hi ADD PARTITION(year=2018,month=05,day=05,hour=10) " +
                "LOCATION 's3a://99taxis-dw-international-online/hive-export/international/dwd_order_pay_success_hi/2018/05/05/10/';" +
                " COMPUTE INCREMENTAL STATS redshift.dwd_order_pay_success_hi PARTITION(year=2018,month=05,day=05,hour=10);"));
    }


    @Test
    public void backFillTable() throws Exception {
        String ddl = service.backfillTable("dwd_order_pay_success_hi");
        assertTrue(ddl.contains("INSERT INTO new_app.dwd_order_pay_success_hi SELECT  CAST(timezone AS bigint) , CAST(country_id AS bigint) , " +
                "CAST(stat_date AS timestamp) , CAST(stat_hour AS string) , CAST(current_stat_date AS string) , CAST(current_stat_hour AS string) , " +
                "CAST(order_id AS bigint) , CAST(city_id AS bigint) , CAST(product_id AS bigint) , CAST(passenger_id AS bigint) , " +
                "CAST(driver_id AS bigint) , CAST(payable_cost AS decimal(38,6)) , CAST(actual_cost AS decimal(38,6)) , CAST(is_use_coupon AS bigint) , " +
                "CAST(coupon_spend AS decimal(38,6)) , CAST(pay_info AS string) , CAST(country_code AS string) , CAST(year AS smallint) , CAST(month AS smallint) , " +
                "CAST(day AS smallint) , CAST(hour AS smallint)  FROM backfill.dwd_order_pay_success_hi "));
    }

    @Test
    public void prepareCommand() throws Exception {
        String ddl = service.prepareCommand("redshift", "new_app", "dwd_order_pay_success_hi", "2018", "05", "05", "10" );
        assertTrue(ddl.contains("ALTER TABLE new_app.dwd_order_pay_success_hi DROP PARTITION (year=2018,month=05,day=05,hour=10); SET compression_codec=snappy; " +
                "SET parquet_file_size=256mb; INSERT INTO new_app.dwd_order_pay_success_hi SELECT  CAST(timezone AS bigint) , CAST(country_id AS bigint) , CAST(stat_date AS timestamp)" +
                " , CAST(stat_hour AS string) , CAST(current_stat_date AS string) , CAST(current_stat_hour AS string) , CAST(order_id AS bigint) , CAST(city_id AS bigint) , " +
                "CAST(product_id AS bigint) , CAST(passenger_id AS bigint) , CAST(driver_id AS bigint) , CAST(payable_cost AS decimal(38,6)) , CAST(actual_cost AS decimal(38,6)) , " +
                "CAST(is_use_coupon AS bigint) , CAST(coupon_spend AS decimal(38,6)) , CAST(pay_info AS string) , CAST(country_code AS string)  FROM redshift.dwd_order_pay_success_hi " +
                "WHERE year=2018 AND month=05 AND day=05 AND hour=10; COMPUTE INCREMENTAL STATS new_app.dwd_order_pay_success_hi PARTITION(year=2018,month=05,day=05,hour=10); "));
    }

    @Test
    public void executeCommand() throws Exception {
        assertTrue(service.executeCommand("SET compression_codec=snappy;"));
    }

}