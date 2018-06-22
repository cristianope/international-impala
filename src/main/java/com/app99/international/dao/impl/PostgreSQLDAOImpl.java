package com.app99.international.dao.impl;


import com.app99.international.dao.ImpalaDAO;
import com.app99.international.dao.PostgreSQLDAO;
import com.app99.international.listener.SQSListener;
import com.app99.international.model.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Repository
public class PostgreSQLDAOImpl extends JdbcDaoSupport implements PostgreSQLDAO {

    private final String FIELDS = "SELECT p.column AS column_name, p.type AS type_name FROM pg_table_def p WHERE tablename = ':table' and schemaname = ':schema'; ";
    private final String PARAMETER =  "SET search_path TO :schema; ";

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLDAOImpl.class);
    @Autowired
    private DataSource pgPoolingDataSource;

    @PostConstruct
    private void initialize(){
        setDataSource(pgPoolingDataSource);
    }

    private List<Field> executeQuery(String sql, String parameter){
        Connection conn = null;
        try {
            conn = pgPoolingDataSource.getConnection();
            conn.setSchema("new_app");
            conn.nativeSQL(parameter);
            return executeQuery(sql, conn);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {}
            }
        }
    }

    private List<Field> executeQuery(String sql, Connection conn) throws SQLException{
        List<Field> fields = new ArrayList<Field>();

        LOGGER.info("executeQuery ======================== SQL: " + sql);

        conn = pgPoolingDataSource.getConnection();
        Statement ps =  conn.createStatement();
        ResultSet rs = ps.executeQuery(sql);
        while (rs.next()) {
            Field field = new Field(
                rs.getString("column_name"),
                verifyField(rs.getString("column_name"), rs.getString("type_name")));
            fields.add(field);
        }
        rs.close();
        ps.close();
        return fields;
    }

    @Override
    public List<Field> getFields(String schema, String tableName) throws Exception {
        String sql = FIELDS.replace(":table", tableName).replace(":schema", schema);
        String parameter = PARAMETER.replace(":schema", schema);
        return executeQuery(sql, parameter);
    }
}
