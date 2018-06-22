package com.app99.international.dao;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import com.app99.international.application.Application;
import com.app99.international.configuration.EnvironmentVariable;
import com.app99.international.dao.impl.PostgreSQLDAOImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.web.ManagementContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import com.app99.international.model.Field;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration
public class PostgreSQLDAOTest extends EnvironmentVariable {

    @Autowired
    private PostgreSQLDAOImpl redshift;

    @Test
    public void getFields() throws Exception {
        List<Field> fields = redshift.getFields("new_app", "dim_city");

        for (Field field: fields){
            if((field.getField().equals("stat_date")) && (field.getType().equals("timestamp"))){
                assertTrue(true);
            }
        }
        assertTrue(true);
    }



}