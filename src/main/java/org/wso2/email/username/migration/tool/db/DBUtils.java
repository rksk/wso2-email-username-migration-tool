package org.wso2.email.username.migration.tool.db;

import org.wso2.email.username.migration.tool.IdentityException;
import org.wso2.email.username.migration.tool.util.ConfigParser;

import java.sql.*;

public class DBUtils {
    private static ConfigParser configParser = null;
    private static Connection regdb_connection = null;
    private static Connection umdb_connection = null;

    public static Connection getREGDBConnection() throws IdentityException {

        if (regdb_connection == null) {
            if (configParser == null) {
                configParser = new ConfigParser();
            }
            String url = configParser.getProperty("REGDB_CONNECTION_URL");
            String username = configParser.getProperty("REGDB_CONNECTION_USERNAME");
            String password = configParser.getProperty("REGDB_CONNECTION_PASSWORD");
            String driverClass = configParser.getProperty("REGDB_CONNECTION_DRIVERCLASS");
            String driverLocation = configParser.getProperty("REGDB_CONNECTION_JDBCDRIVER");

            DBConnection.loadDBDriver(driverLocation, driverClass);
            regdb_connection = DBConnection.getConnection(url, username, password);
        }
        return regdb_connection;
    }

    public static Connection getUMDBConnection() throws IdentityException {

        if (umdb_connection == null) {
            if (configParser == null) {
                configParser = new ConfigParser();
            }
            String url = configParser.getProperty("UMDB_CONNECTION_URL");
            String username = configParser.getProperty("UMDB_CONNECTION_USERNAME");
            String password = configParser.getProperty("UMDB_CONNECTION_PASSWORD");
            String driverClass = configParser.getProperty("UMDB_CONNECTION_DRIVERCLASS");
            String driverLocation = configParser.getProperty("UMDB_CONNECTION_JDBCDRIVER");

            DBConnection.loadDBDriver(driverLocation, driverClass);
            umdb_connection = DBConnection.getConnection(url, username, password);
        }
        return umdb_connection;
    }

    public static void closeResultSetAndStatement(ResultSet rs, PreparedStatement prepStmt) {

        closeResultSet(rs);
        closeStatement(prepStmt);
    }

    public static void closeConnection(Connection dbConnection) {
        if (dbConnection != null) {
            try {
                dbConnection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeStatement(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeStatement(PreparedStatement[] prepStmts) {
        if (prepStmts != null) {
            for (PreparedStatement prepStmt : prepStmts) {
                closeStatement(prepStmt);
            }
        }
    }
}
