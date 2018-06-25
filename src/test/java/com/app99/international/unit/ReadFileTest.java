package com.app99.international.service;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import com.app99.international.application.Application;
import com.app99.international.model.ReadFile;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import com.app99.international.model.Field;

@RunWith(SpringRunner.class)
public class ReadFileTest {

    @Test
    public void getFile() throws Exception {
        assertTrue(!(new ReadFile().getFile( "tables_full_partition").isEmpty()));
        assertTrue((new ReadFile().getFile( "tables_partitions").size() > 0));
    }

}