package com.app99.international.dao.impl;

import com.app99.international.dao.HiveMetastoreDAO;
import com.app99.international.model.Field;
import com.app99.international.model.ReadFile;
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

/**
 * Created by vinicius.aquino on 6/20/18.
 */
@Repository
public class HiveMetastoreDAOImpl extends JdbcDaoSupport implements HiveMetastoreDAO {

    private final String FIELDS = "SELECT column_name,  type_name FROM COLUMNS_V2 c JOIN TBLS a ON c.CD_ID=a.TBL_ID" +
            " WHERE a.TBL_ID = (SELECT TBL_ID FROM TBLS WHERE DB_ID = (SELECT DB_ID FROM DBS WHERE NAME = ':database')" +
            " AND TBL_NAME = ':table')  ORDER BY INTEGER_IDX; ";

    private final String PARTITIONS = "SELECT PKEY_NAME as column_name,  PKEY_TYPE as type_name FROM PARTITION_KEYS WHERE DB_ID = " +
            "(SELECT DB_ID FROM DBS WHERE NAME = ':database') AND TBL_NAME = ':table')  ORDER BY INTEGER_IDX; ";


    @Autowired
    private DataSource myPoolingDataSource;

    @PostConstruct
    private void initialize(){
        setDataSource(myPoolingDataSource);
    }


    @Override
    public List<Field> getFieldsPartitions(String database, String tableName){
        String sql = PARTITIONS.replace(":database", database).replace(":table", tableName);
        return executeQuery(sql);
    }

    @Override
    public boolean isFullPartition(String tableName) {
        List<String> tables = new ReadFile().getFile("tables_full_partition");

        for (String table:tables) {
            if (tableName.equals(table)){
                return true;
            }
        }
        return false;
    }

    @Override
    public List<Field> getFields(String database, String tableName){
        String sql = FIELDS.replace(":database", database).replace(":table", tableName);
        return executeQuery(sql);
    }

    private List<Field> executeQuery(String sql){
        List<Field> fields = new ArrayList<Field>();

        Connection conn = null;
        try {
            conn = myPoolingDataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                fields.add(new Field(
                        rs.getString("column_name"),
                        rs.getString("type_name")
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
}
