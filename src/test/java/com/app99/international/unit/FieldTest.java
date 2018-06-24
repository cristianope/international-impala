package com.app99.international.service;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import com.app99.international.application.Application;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import com.app99.international.model.Field;

@RunWith(SpringRunner.class)
public class FieldTest {

    @Test
    public void toCreateDDL() throws Exception {
        Field field = new Field("column_name", "type_name");

        assertThat(field.toCreateDDL("test"), equalTo("column_name timestamp"));
    }

    @Test
    public void toParquet() throws Exception {
        Field field = new Field("column_name", "type_name");

        assertThat(field.toParquet("test"), equalTo(" CAST(column_name AS timestamp) "));
    }

    @Test
    public void toEqual() throws Exception {
        Field field = new Field("column_name", "type_name");
        Field field2 = new Field("column_name", "type_name");
        Field field3 = new Field("column_name2", "type_name");
        Field field4 = new Field("column_name2", "type_name2");

        assertTrue(field.equals(field2));

        assertFalse(field.equals(field3));

    }

}