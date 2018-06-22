package com.app99.international.dao.impl;


import com.app99.international.dao.ImpalaDAO;
import com.app99.international.dao.PostgreSQLDAO;
import com.app99.international.model.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Repository
public class PostgreSQLDAOImpl extends JdbcDaoSupport implements PostgreSQLDAO {

    private final String FIELDS = "SET search_path TO :schema; SELECT p.column AS column_name, p.type AS type_name FROM pg_table_def p WHERE tablename = ':table'; ";

    @Autowired
    private DataSource pgPoolingDataSource;

    @PostConstruct
    private void initialize(){
        setDataSource(pgPoolingDataSource);
    }


    private String verifyField(String field, String typeName){
        if(typeName.startsWith("character")){
            return "string";
        }else{
            if(typeName.startsWith("numeric")){
                return typeName.replace("numeric", "decimal");
            }else{
                if(typeName.startsWith("integer")){
                    return typeName.replace("integer","int");
                }
            }
        }

        if(field.equals("year")){
            return "smallint";
        }else{
            if(field.contains("month,day,hour")){
                return "tinyint";
            }
        }

        return typeName;
    }

    private List<Field> executeQuery(String sql){
        List<Field> fields = new ArrayList<Field>();

        Connection conn = null;
        try {
            conn = pgPoolingDataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                fields.add(new Field(
                        rs.getString("column_name"),
                        verifyField(rs.getString("column_name"), rs.getString("type_name"))
                ));
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
        String sql = FIELDS.replace(":schema", schema).replace(":table", tableName);
        return executeQuery(sql);
    }
}
