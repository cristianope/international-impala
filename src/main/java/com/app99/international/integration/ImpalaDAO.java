package com.app99.international.integration;

import java.sql.SQLException;
import java.util.List;

public interface ImpalaDAO  extends DAO{


   List<Boolean> executeQuery(String sql) throws SQLException;

}