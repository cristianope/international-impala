package com.app99.international.dao;

import com.app99.international.model.Field;

import java.util.List;

/**
 * Created by vinicius.aquino on 6/20/18.
 */
public interface HiveMetastoreDAO {

    boolean isFullPartition(String tableName);

    List<Field> getFields(String database, String tableName);

    List<Field> getFieldsPartitions(String database, String tableName);
}
