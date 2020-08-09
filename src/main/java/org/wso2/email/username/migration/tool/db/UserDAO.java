package org.wso2.email.username.migration.tool.db;

import org.wso2.email.username.migration.tool.IdentityException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class UserDAO {

    public static HashMap getUserListToRename(int chunkSize) throws IdentityException {

        Connection connection = DBUtils.getUMDBConnection();
        PreparedStatement prepStmt = null;
        ResultSet rSet = null;
        HashMap userList = new HashMap<String, String>();
        try {
            prepStmt = connection.prepareStatement(SQLQueries.USER_LIST_TO_RENAME_SQL);
            prepStmt.setInt(1, chunkSize);
            rSet = prepStmt.executeQuery();
            while (rSet.next()) {
                String uuid = UUID.randomUUID().toString();
                userList.put(rSet.getString(1), uuid);
            }
        } catch (SQLException e) {
            throw new IdentityException("Error when reading the User List to be renamed from the user store.", e);
        } finally {
            DBUtils.closeResultSetAndStatement(rSet, prepStmt);
        }
        return userList;
    }

    public static String getEmailOfUser(String username) throws IdentityException {

        Connection connection = DBUtils.getUMDBConnection();
        PreparedStatement prepStmt = null;
        ResultSet rSet = null;
        String email = null;
        try {
            prepStmt = connection.prepareStatement(SQLQueries.SELECT_USER_EMAIL_SQL);
            prepStmt.setString(1, username);
            rSet = prepStmt.executeQuery();
            while (rSet.next()) {
                email = rSet.getString(1);
            }
        } catch (SQLException e) {
            throw new IdentityException("Error when reading email address of user " + username, e);
        } finally {
            DBUtils.closeResultSetAndStatement(rSet, prepStmt);
        }
        return email;
    }

    public static void updateEmailOfUser(HashMap userList) throws IdentityException {

        Connection connection = DBUtils.getUMDBConnection();
        PreparedStatement prepStmt = null;
        try {
            prepStmt = connection.prepareStatement(SQLQueries.UPDATE_USER_EMAIL_SQL);
            for(Object entry : userList.entrySet()) {
                String username = (String)((Map.Entry)entry).getKey();
                String email = (String)((Map.Entry)entry).getValue();
                prepStmt.setString(1, email);
                prepStmt.setString(2, username);
                prepStmt.addBatch();
            };
            prepStmt.executeBatch();
        } catch (SQLException e) {
            throw new IdentityException("Error while updating email addresses.", e);
        } finally {
            DBUtils.closeResultSetAndStatement(null, prepStmt);
        }
    }

    public static void addEmailOfUser(HashMap userList) throws IdentityException {

        Connection connection = DBUtils.getUMDBConnection();
        PreparedStatement prepStmt = null;
        try {
            prepStmt = connection.prepareStatement(SQLQueries.ADD_USER_EMAIL_SQL);
            for(Object entry : userList.entrySet()) {
                String username = (String)((Map.Entry)entry).getKey();
                String email = (String)((Map.Entry)entry).getValue();
                prepStmt.setString(1, username);
                prepStmt.setString(2, email);
                prepStmt.addBatch();
            };
            prepStmt.executeBatch();
        } catch (SQLException e) {
            throw new IdentityException("Error while inserting email addresses.", e);
        } finally {
            DBUtils.closeResultSetAndStatement(null, prepStmt);
        }
    }

    public static void updateUMtables(HashMap userList) throws IdentityException {

        Connection connection = DBUtils.getUMDBConnection();
        PreparedStatement[] prepStmts = new PreparedStatement[3];
        try {
            prepStmts[0] = connection.prepareStatement(SQLQueries.UPDATE_UM_USER_SQL);
            prepStmts[1] = connection.prepareStatement(SQLQueries.UPDATE_UM_HYBRID_REMEMBER_ME_SQL);
            prepStmts[2] = connection.prepareStatement(SQLQueries.UPDATE_UM_HYBRID_USER_ROLE_SQL);
            for(Object entry : userList.entrySet()) {
                String oldUsername = (String)((Map.Entry)entry).getKey();
                String newUsername = (String)((Map.Entry)entry).getValue();
                for (PreparedStatement prepStmt : prepStmts) {
                    prepStmt.setString(1, newUsername);
                    prepStmt.setString(2, oldUsername);
                    prepStmt.addBatch();
                }
            };
            for (PreparedStatement prepStmt : prepStmts) {
                prepStmt.executeBatch();
            }
        } catch (SQLException e) {
            throw new IdentityException("Error while updating UM tables", e);
        } finally {
            DBUtils.closeStatement(prepStmts);
        }
    }


    public static void updateIDNtables(HashMap userList) throws IdentityException {

        Connection connection = DBUtils.getREGDBConnection();
        PreparedStatement[] prepStmts = new PreparedStatement[5];
        try {
            prepStmts[0] = connection.prepareStatement(SQLQueries.UPDATE_CM_RECEIPT_SQL);
            prepStmts[1] = connection.prepareStatement(SQLQueries.UPDATE_IDN_ASSOCIATED_ID_SQL);
            prepStmts[2] = connection.prepareStatement(SQLQueries.UPDATE_IDN_IDENTITY_USER_DATA_SQL);
            prepStmts[3] = connection.prepareStatement(SQLQueries.UPDATE_IDN_RECOVERY_DATA_SQL);
            prepStmts[4] = connection.prepareStatement(SQLQueries.UPDATE_SP_APP_SQL);
            for(Object entry : userList.entrySet()) {
                String oldUsername = (String)((Map.Entry)entry).getKey();
                String newUsername = (String)((Map.Entry)entry).getValue();
                for (PreparedStatement prepStmt : prepStmts) {
                    prepStmt.setString(1, newUsername);
                    prepStmt.setString(2, oldUsername);
                    prepStmt.addBatch();
                }
            };
            for (PreparedStatement prepStmt : prepStmts) {
                prepStmt.executeBatch();
            }
        } catch (SQLException e) {
            throw new IdentityException("Error while updating IDN tables", e);
        } finally {
            DBUtils.closeStatement(prepStmts);
        }
    }

    public static void updateREGtables(HashMap userList) throws IdentityException {

        Connection connection = DBUtils.getREGDBConnection();
        PreparedStatement prepStmt = null;
        try {
            prepStmt = connection.prepareStatement(SQLQueries.UPDATE_REG_PROPERTY_SQL);
            for(Object entry : userList.entrySet()) {
                String oldUsername = (String)((Map.Entry)entry).getKey();
                String newUsername = (String)((Map.Entry)entry).getValue();
                prepStmt.setString(1, newUsername);
                prepStmt.setString(2, oldUsername);
                prepStmt.addBatch();
             };
            prepStmt.executeBatch();
        } catch (SQLException e) {
            throw new IdentityException("Error while updating REG_PROPERTY table", e);
        } finally {
            DBUtils.closeResultSetAndStatement(null, prepStmt);
        }
    }
}
