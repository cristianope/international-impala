package com.app99.international.dao;

import com.app99.international.application.Application;
import com.app99.international.configuration.EnvironmentVariable;
import com.app99.international.dao.impl.HiveMetastoreDAOImpl;
import com.app99.international.dao.impl.PostgreSQLDAOImpl;
import com.app99.international.model.Field;
import com.app99.international.service.impl.BasicCommands;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import sun.rmi.runtime.Log;

import java.util.List;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration
public class HiveMetastoreDAOTest extends EnvironmentVariable {

    @Autowired
    private HiveMetastoreDAOImpl catalog;

    @Test
    public void getFields() throws Exception {
        List<Field> fields = catalog.getFields("redshift", "dim_city");
        Field result = null;

        for (Field field: fields){
            if(field.getField().equals("stat_date") && field.getType().equals("timestamp")){
                result = field;
            }
        }
        assertTrue(result != null);
    }

    @Test
    public void getFieldsPartitions() throws Exception {
        List<Field> fields = catalog.getFieldsPartitions("redshift", "dim_city");
        Field result = null;

        for (Field field: fields){
            if(field.getField().equals("year") && field.getType().equals("smallint")){
                result = field;
            }
        }
        assertTrue(result != null);
    }


    @Test
    public void isFullPartition() throws Exception {
        assertTrue(catalog.isFullPartition("app_order_base_mx_d"));
    }

}