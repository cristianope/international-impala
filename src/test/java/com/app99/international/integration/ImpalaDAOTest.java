package com.app99.international.integration;

import com.app99.international.application.Application;
import com.app99.international.environment.EnvironmentVariable;
import com.app99.international.integration.impl.ImpalaDAOImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
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

    @Test
    public void executeCommandDDL() throws Exception {
        assertFalse(impala.executeQuery("SET compression_codec=snappy; SET parquet_file_size=256mb; "));
    }

    @Test
    public void executeCommandDQL() throws Exception {
        assertTrue(impala.executeQuery("SELECT * FROM new_app.dim_city; SELECT * FROM redshift.dim_city;"));
    }

    @Test
    public void executeCommandDQLTableNotExist() throws Exception {
        expectedEx.expect(SQLException.class);
        expectedEx.expectMessage("[Simba][ImpalaJDBCDriver](500051) ERROR processing query/statement. " +
                "Error Code: 0, SQL state: TStatus(statusCode:ERROR_STATUS, sqlState:HY000, errorMessage:AnalysisException: " +
                "Could not resolve table reference: 'new_app.table_not_exist'\n" +
                "), Query: SELECT * FROM new_app.table_not_exist;.");
        assertFalse(impala.executeQuery("SELECT * FROM new_app.table_not_exist; "));
    }
}
