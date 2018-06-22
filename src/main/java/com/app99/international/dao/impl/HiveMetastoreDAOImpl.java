package com.app99.international.dao.impl;

import com.app99.international.dao.HiveMetastoreDAO;
import com.app99.international.model.Field;
import com.app99.international.model.ReadFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private final String PARTITIONS = "SELECT PKEY_NAME as column_name,  PKEY_TYPE as type_name FROM PARTITION_KEYS" +
            " WHERE TBL_ID = (SELECT TBL_ID FROM TBLS WHERE DB_ID = (SELECT DB_ID FROM DBS WHERE " +
            "NAME = ':database') AND TBL_NAME = ':table') ORDER BY INTEGER_IDX;";

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveMetastoreDAOImpl.class);

    @Autowired
    private DataSource myPoolingDataSource;

    @PostConstruct
    private void initialize(){
        setDataSource(myPoolingDataSource);
    }


    @Override
    public List<Field> getFieldsDDL(String database, String tableName) throws Exception{

        List<Field> fields = getFields(database, tableName);
        List<Field> partitions = getFieldsPartitions(database, tableName);
        List<Field> partitions2 = getFieldsPartitionsFile(tableName);

        partitions.removeAll(partitions2);

        fields.addAll(partitions);

        return fields;
    }

    @Override
    public List<Field> getFieldsPartitions(String database, String tableName) throws Exception{
        String sql = PARTITIONS.replace(":database", database).replace(":table", tableName);
        return executeQuery(sql, tableName);
    }

    @Override
    public List<Field> getFieldsPartitionsFile(String tableName) throws Exception {
        List<String> linhas = new ReadFile().getFile("tables_partitions");
        List<Field> fields = new ArrayList<Field>();

        for (String linha : linhas){
            String[] columns = linha.split("=");
            String table = columns[0];

            if (tableName.equals(table)){
                String[] tuples = columns[1].split(",");

                for(String tuple: tuples) {
                    Field field = null;
                    if(tuple.equals("year")){
                        field = new Field(tuple, "smallint");
                    }else{
                        field = new Field(tuple, "tinyint");
                    }
                fields.add(field);
            }
            }
        }
        return fields;
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

        return executeQuery(sql, tableName);
    }

    private List<Field> executeQuery(String sql, String tableName){
        List<Field> fields = new ArrayList<Field>();

        LOGGER.info("executeQuery ======================== SQL: " + sql);

        Connection conn = null;
        try {
            conn = myPoolingDataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Field field = new Field(
                        rs.getString("column_name"),
                        verifyField(rs.getString("column_name"), rs.getString("type_name"))
                );
                field.updateFieldsNewDataTypes(tableName);
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
}
