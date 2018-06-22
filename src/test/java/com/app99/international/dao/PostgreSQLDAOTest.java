/*package com.app99.international.dao;

import static org.junit.Assert.*;

import com.app99.international.application.Application;
import com.app99.international.configuration.EnvironmentVariable;
import com.app99.international.dao.impl.RedshiftDAOImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import com.app99.international.model.Field;
import sun.rmi.runtime.Log;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration
public class PostgreSQLDAOTest extends EnvironmentVariable {

    @Autowired
    private RedshiftDAOImpl redshift;

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLDAOTest.class);

    @Test
    public void getFields() throws Exception {
        List<Field> fields = redshift.getFields("new_app", "dim_city");
        boolean result = false;
        for (Field field: fields){
            field.updateFieldsNewDataTypes("dim_city");
            if((field.getField().equals("stat_date")) && (field.getType().equals("timestamp"))){
                result = true;
                break;
            }
        }
        assertTrue(result);
    }
}
*/