/*
package com.app99.international.dao.impl;


import com.app99.international.dao.ImpalaDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
    public boolean executeCommand(String sql) throws Exception {

        Connection conn = null;
        LOGGER.info("executeQuery ======================== SQL: " + sql);

        try {
            conn = impalaPoolingDataSource.getConnection();
            Statement ps = conn.createStatement();
            ps.executeQuery(sql);
            ps.close();
        } catch (SQLException e) {
            throw new Exception(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {}
            }
        }
        return true;
    }
}
*/