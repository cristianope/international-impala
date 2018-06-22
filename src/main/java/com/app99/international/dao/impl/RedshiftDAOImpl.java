package com.app99.international.dao.impl;


import com.app99.international.dao.RedshiftDAO;
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
public class RedshiftDAOImpl extends JdbcDaoSupport implements RedshiftDAO {

    private final String FIELDS = "SELECT p.column AS column_name, p.type AS type_name FROM pg_table_def p WHERE tablename = ':table' and schemaname = ':schema'; ";
    private final String PARAMETER =  "SET search_path TO :schema; ";

    private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftDAOImpl.class);
    @Autowired
    private DataSource pgPoolingDataSource;

    @PostConstruct
    private void initialize(){
        setDataSource(pgPoolingDataSource);
    }

    private List<Field> executeQuery(String sql, String parameter){
        List<Field> fields = new ArrayList<Field>();
        Connection conn = null;
        try {
            conn = pgPoolingDataSource.getConnection();
            conn.setSchema("new_app");

            Statement ps =  conn.createStatement();
            ps.execute(parameter);
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

    @Override
    public List<Field> getFields(String schema, String tableName) throws Exception {
        Field year = new Field("year", "smallint");
        Field month = new Field("month", "tinyint");
        Field day = new Field("day", "tinyint");
        Field hour = new Field("hour", "tinyint");

        String sql = FIELDS.replace(":table", tableName).replace(":schema", schema);
        String parameter = PARAMETER.replace(":schema", schema);
        List<Field> result = executeQuery(sql, parameter);

        result.remove(year);
        result.remove(month);
        result.remove(day);
        result.remove(hour);

        return result;
    }
}
