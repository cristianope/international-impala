package com.app99.international.unit;

import static org.junit.Assert.*;

import com.app99.international.model.Functions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class FunctionsTest {

    @Test
    public void getFile() throws Exception {
        assertTrue(!(Functions.getFile( "tables_full_partition").isEmpty()));
        assertTrue((Functions.getFile( "tables_partitions").size() > 0));
    }

    @Test
    public void startJob() throws Exception {
        assertFalse(Functions.startJob("01-07-2018"));
        assertTrue(Functions.startJob("01-07-2017"));
    }
}