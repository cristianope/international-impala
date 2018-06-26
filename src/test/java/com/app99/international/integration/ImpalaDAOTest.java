package com.app99.international.integration;

import com.app99.international.application.Application;
import com.app99.international.environment.EnvironmentVariable;
import com.app99.international.integration.impl.ImpalaDAOImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;


import java.sql.SQLException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@ContextConfiguration
public class ImpalaDAOTest extends EnvironmentVariable {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Autowired
    private ImpalaDAOImpl impala;

    private static final Logger LOGGER = LoggerFactory.getLogger(ImpalaDAOTest.class);

      @Test
    public void executeCommandDDLTable() throws Exception {
        for(Boolean bool: impala.executeQuery("CREATE TABLE IF NOT EXISTS default.teste(id bigint) STORED AS PARQUET; COMPUTE STATS default.teste; ")){
            assertTrue(bool);
        }
    }

    @Test
    public void executeCommandDDL() throws Exception {
        for(Boolean bool: impala.executeQuery("SET compression_codec=snappy; SET parquet_file_size=256mb; ")){
            assertTrue(bool);
        }
    }



    @Test
    public void executeCommandDQL() throws Exception {
        assertTrue(impala.executeQuery("SELECT * FROM default.teste2;").get(0));
    }

    @Test
    public void executeCommandDDLWrong() throws SQLException {
        expectedEx.expect(SQLException.class);
        for (Boolean bool : impala.executeQuery("SET compression_codec=coisa_sem_sentido; SET parquet_file_size=sei_la; ")) {
            assertFalse(bool);
        }
    }

    @Test
    public void executeCommandDQLTableNotExist() throws SQLException {
        expectedEx.expect(SQLException.class);
        assertFalse(impala.executeQuery("SELECT * FROM new_app.table_not_exist; ").get(0));
    }
}
