package com.app99.international.service;

public interface ImpalaService {

    String AddPartitionsS3(String oldDatabase, String tableName, String[] values) throws Exception;

    String prepareCommand(String oldDatabase, String database, String tableName, String year, String month, String day, String hour) throws Exception;

    boolean executeQuery(String command) throws Exception;

    String backfillTable(String tableName) throws Exception;

}