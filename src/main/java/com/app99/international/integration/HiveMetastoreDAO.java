package com.app99.international.integration;

import com.app99.international.model.Field;

import java.util.List;

/**
 * Created by vinicius.aquino on 6/20/18.
 */
public interface HiveMetastoreDAO extends DAO{

    boolean isFullPartition(String tableName) throws Exception;

    List<Field> getFields(String database, String tableName) throws  Exception;

    List<Field> getFieldsPartitions(String database, String tableName) throws Exception;

    List<Field> getFieldsPartitionsFile(String tableName) throws Exception;

    List<Field> getFieldsDDL(String database, String tableName) throws Exception;
}
