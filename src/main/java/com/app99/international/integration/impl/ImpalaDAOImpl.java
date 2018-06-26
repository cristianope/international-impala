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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Repository
public class ImpalaDAOImpl  extends JdbcDaoSupport implements ImpalaDAO {

    @Autowired
    private DataSource impalaPoolingDataSource;

    @PostConstruct
    private void initialize(){
        setDataSource(impalaPoolingDataSource);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ImpalaDAOImpl.class);


    private boolean runQuery(String query, Statement ps) throws SQLException {
        if(query.contains("INSERT") || query.contains("ALTER") || query.contains("CREATE") || query.contains("TRUNCATE") || query.contains("SET")) {
            if (query.contains("INSERT")) {
                return (ps.executeUpdate(query + ";") > 0) ;
            } else {
                return !(ps.executeUpdate(query + ";") == 0);
            }
        }else {
            ResultSet rs = ps.executeQuery(query + ";");
            return (rs.next());
        }
    }


    @Override
    public List<Boolean> executeQuery(String sql) throws SQLException {
        String[] queries = sql.trim().split(";");
        List<Boolean> results = new ArrayList<>();

        Connection conn = null;

        try {
            LOGGER.info("executeQuery ===================== SQL FULL: " + sql );
            conn = impalaPoolingDataSource.getConnection();
            Statement ps = conn.createStatement();

            for(String query: queries){
                if(conn.isValid(5000)) {
                    results.add(runQuery(query, ps));
                }else {
                    LOGGER.warn("executeQuery ===================== SQL - Reopen Connection");
                    conn = impalaPoolingDataSource.getConnection();
                    ps = conn.createStatement();
                    results.add(runQuery(query, ps));
                }
            }
            ps.close();
            return results;
        } catch (SQLException e) {
            LOGGER.warn("executeQuery ======================== Exception: " + e.getMessage());
            throw new SQLException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) { throw new SQLException(e);}
            }
        }
    }
}
