package com.app99.international.integration;

import com.app99.international.model.Field;

import java.util.List;

public interface RedshiftDAO extends DAO{

    List<Field> getFields(String schema, String tableName) throws Exception;

}