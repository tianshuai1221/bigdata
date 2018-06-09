package com.atguigu.util;

import java.sql.Connection;
import java.sql.SQLException;

public class JDBCInstance {

    private static Connection connection = null;

    private JDBCInstance() {
    }

    public static Connection getInstance() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = JDBCUtil.getConnection();
        }
        return connection;
    }
}
