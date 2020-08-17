package org.wso2.email.username.migration.tool;

import org.wso2.email.username.migration.tool.db.UserDAO;
import org.wso2.email.username.migration.tool.db.DBUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

class EmailUsernameMigrator {

    private static int usercount;
    private static final int ITERATION_SIZE = 1000;
    private static final int THRESHOLD = 9999999;
    private static final boolean DEBUG = true;

    public static void main(String[] args) {
        Long startTime = System.currentTimeMillis();
        new EmailUsernameMigrator().runMigration();
        System.out.println("\n\n=== " + usercount + " users were migrated in " + (System.currentTimeMillis() - startTime) + "ms ===");
    }

    private void runMigration() {

        try {
            // Test DB connections
            Long conStartTime = System.currentTimeMillis();
            Connection regdb_connection = DBUtils.getREGDBConnection();
            Connection umdb_connection = DBUtils.getUMDBConnection();
            if (DEBUG) System.out.println(" -- DB connections were obtained in " + (System.currentTimeMillis() - conStartTime) + "ms.");

            // Get user list chunks and do migration
            while (true) {
                Long startTime = System.currentTimeMillis();
                HashMap userList = UserDAO.getUserListToRename(ITERATION_SIZE);
                if (DEBUG) System.out.println(" -- Time to get userlist " + (System.currentTimeMillis() - startTime) + "ms.");
                if (userList.size() == 0 || usercount >= THRESHOLD) {
                    break;
                }
                Long iterationStartTime = System.currentTimeMillis();
                populateEmailClaim(userList);
                if (DEBUG) System.out.println(" -- Populating emails done in " + (System.currentTimeMillis() - iterationStartTime) + "ms.");

                iterationStartTime = System.currentTimeMillis();
                UserDAO.updateIDNtables(userList);
                if (DEBUG) System.out.println(" -- Updating IDN tables done in " + (System.currentTimeMillis() - iterationStartTime) + "ms.");

                iterationStartTime = System.currentTimeMillis();
                UserDAO.updateREGtables(userList);
                if (DEBUG) System.out.println(" -- Updating REG tables done in " + (System.currentTimeMillis() - iterationStartTime) + "ms.");

                iterationStartTime = System.currentTimeMillis();
                UserDAO.updateUMtables(userList);
                if (DEBUG) System.out.println(" -- Updating UM tables done in " + (System.currentTimeMillis() - iterationStartTime) + "ms.");

                iterationStartTime = System.currentTimeMillis();
                regdb_connection.commit();
                umdb_connection.commit();
                if (DEBUG) System.out.println(" -- Committing changes done in " + (System.currentTimeMillis() - iterationStartTime) + "ms.");

                usercount += userList.size();
                System.out.println(userList.size() + " users were migrated in " + (System.currentTimeMillis() - startTime) + "ms");
            }
            
        } catch (IdentityException | SQLException e) {
            System.out.println("=== An error occurred! ===");
            e.printStackTrace();
            System.out.println("=======================");
        } finally {
            // close DB connection at the end
            try {
                DBUtils.closeConnection(DBUtils.getREGDBConnection());
                DBUtils.closeConnection(DBUtils.getUMDBConnection());
            } catch (IdentityException | SQLException e) {
                System.out.println("=== An error occurred while closing the DB connection ===");
                e.printStackTrace();
                System.out.println("=======================");
            }
        }
    }

    private void populateEmailClaim(HashMap<String, String> userList) throws IdentityException, SQLException {

        HashMap toInsert = new HashMap<String, String>();
        HashMap toUpdate = new HashMap<String, String>();
        for(Map.Entry<String, String> entry : userList.entrySet()) {
            String username = entry.getKey();
            String existingEmail = UserDAO.getEmailOfUser(username);
            String newEmail = username.trim();
            if (existingEmail == null) {
                toInsert.put(username, newEmail);
            } else if (!existingEmail.equals(newEmail)) {
                toUpdate.put(username, newEmail);
            }
        };
        UserDAO.addEmailOfUser(toInsert);
        UserDAO.updateEmailOfUser(toUpdate);
    }

}
