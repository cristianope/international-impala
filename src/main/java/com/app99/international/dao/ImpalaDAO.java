package com.app99.international.dao;

/**
 * Created by vinicius.aquino on 6/20/18.
 */
public interface ImpalaDAO  extends DAO{


   boolean executeCommand(String sql) throws Exception;

}