package com.app99.international.integration.impl;


import com.app99.international.integration.ImpalaDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Repository
public class ImpalaDAOImpl  extends JdbcDaoSupport implements ImpalaDAO {

    @Autowired
    private DataSource impalaPoolingDataSource;

    @PostConstruct
    private void initialize(){
        setDataSource(impalaPoolingDataSource);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ImpalaDAOImpl.class);


    @Override
    public boolean executeQuery(String sql) throws Exception {
        String[] queries = sql.trim().split(";");

        Connection conn = null;
        boolean result = false;

        try {
            conn = impalaPoolingDataSource.getConnection();
            Statement ps = conn.createStatement();
            for(String query: queries){
                if(conn.isValid(5000)) {
                    LOGGER.info("executeQuery ===================== SQL: " + query );
                    if(query.contains("CREATE") || query.contains("SET") || query.contains("ALTER")){
                        conn.nativeSQL(query);
                    }else{
                        result = ps.execute(query + ";");
                    }
                }else{
                    LOGGER.warn("executeQuery ===================== SQL - Reopen Connection");
                    conn = impalaPoolingDataSource.getConnection();
                }
            }
            ps.close();
            return result;
        } catch (SQLException e) {
            LOGGER.info("executeQuery ======================== Exception: " + e.getMessage());
            throw new Exception(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {}
            }
        }
    }
}
