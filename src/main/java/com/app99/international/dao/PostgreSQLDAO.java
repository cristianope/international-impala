package com.app99.international.dao;

import com.app99.international.model.Field;

import java.util.List;

public interface PostgreSQLDAO {

    List<Field> getFields(String schema, String tableName) throws Exception;

}