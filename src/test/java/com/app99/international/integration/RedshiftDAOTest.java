package com.app99.international.integration;

import static org.junit.Assert.*;

import com.app99.international.application.Application;
import com.app99.international.environment.EnvironmentVariable;
import com.app99.international.integration.impl.RedshiftDAOImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import com.app99.international.model.Field;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration
public class RedshiftDAOTest extends EnvironmentVariable {

    @Autowired
    private RedshiftDAOImpl redshift;

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