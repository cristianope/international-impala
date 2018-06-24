package com.app99.international.integration;

import com.app99.international.application.Application;
import com.app99.international.environment.EnvironmentVariable;
import com.app99.international.integration.impl.ImpalaDAOImpl;
import com.app99.international.model.Field;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration
public class ImpalaDAOTest extends EnvironmentVariable {

    @Autowired
    private ImpalaDAOImpl impala;

    @Test
    public void executeCommandDDL() throws Exception {
        assertFalse(impala.executeQuery("SET compression_codec=snappy; SET parquet_file_size=256mb; "));
    }

    @Test
    public void executeCommandDQL() throws Exception {
        assertTrue(impala.executeQuery("SELECT 1;"));
    }
}
