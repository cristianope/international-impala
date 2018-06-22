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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration
public class BasicCommandsTest extends EnvironmentVariable {

    @Autowired
    private DemonstrationTest service;

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicCommands.class);

    @Test
    public void externalTable() throws Exception {
         String ddl = service.externalTable();
        LOGGER.info("========== DDL " + ddl);
        assertTrue(ddl.contains("ALTER TABLE redshift.dwd_order_pay_success_hi ADD PARTITION(year=2018,month=05,day=05,hour=10) " +
                "LOCATION 's3a://99taxis-dw-international-online/hive-export/international/dwd_order_pay_success_hi/2018/05/05/10/';" +
                " COMPUTE INCREMENTAL STATS redshift.dwd_order_pay_success_hi PARTITION(year=2018,month=05,day=05,hour=10);"));
    }


}

class DemonstrationTest extends BasicCommands{
   public String externalTable() throws Exception {
       try {
           return createTable("redshift", "dim_city", true, existTable("new_app", "dim_city", false));
       } catch (Exception e) {
           e.printStackTrace();
       }
       return null;
   }

}