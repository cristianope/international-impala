package com.app99.international.integration;

public interface ImpalaDAO  extends DAO{


   boolean executeQuery(String sql) throws Exception;

}