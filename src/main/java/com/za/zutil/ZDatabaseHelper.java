package com.za.zutil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class ZDatabaseHelper {

    private final String TAG = "[DBHelper]\t";
    private final String DB_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private final String DB_URL;
    private final String DB_USERNAME;
    private final String DB_PASSWORD;

    private Connection connection;
    private PreparedStatement preparedStatement;

    public String getDB_URL() {
        return DB_URL;
    }

    public ZDatabaseHelper() throws ClassNotFoundException, SQLException {
        DB_URL = "jdbc:mysql://localhost/zanalytics";
        DB_USERNAME = "root";
        DB_PASSWORD = "123456";
        open();
    }

    public ZDatabaseHelper(String address, String username, String password) throws ClassNotFoundException, SQLException {
        DB_URL = String.format("jdbc:mysql://%s/zanalytics", address);
        DB_USERNAME = username;
        DB_PASSWORD = password;
        open();
    }

    private void open() throws ClassNotFoundException, SQLException {
        Class.forName(DB_DRIVER_CLASS);
        connection = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
    }

    public void close() {
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.err.println(TAG + "Error when closing connection: " + e.getMessage());
        }
    }

    public boolean insertIntoPageOverview(String appId, String path, int pageViews,
            int uniquePageViews, int bounces, int entrances, int exits, long totalTimeOnPage) {
        String sql = "insert into page_overview value (default, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, appId);
            preparedStatement.setString(2, path);
            preparedStatement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            preparedStatement.setInt(4, pageViews);
            preparedStatement.setInt(5, uniquePageViews);
            preparedStatement.setInt(6, bounces);
            preparedStatement.setInt(7, entrances);
            preparedStatement.setInt(8, exits);
            preparedStatement.setLong(9, totalTimeOnPage);

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.err.println(TAG + "error when insert to page view: " + e.getMessage());
            return false;
        }
        return true;
    }

    public boolean insertIntoReferal(String appId, String urlRef, int refType, int sessions) {
        String sql = "insert into referal value (default, ?, ?, ?, ?, ?)";
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, appId);
            preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            preparedStatement.setString(3, urlRef);
            preparedStatement.setInt(4, refType);
            preparedStatement.setInt(5, sessions);

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.err.println(TAG + "error when insert into referal: " + e.getMessage());
        }
        return true;
    }

    public boolean insertIntoOs(String appId, String osType, int sessions) {
        String sql = "insert into os value (default, ?, ?, ?, ?)";
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, appId);
            preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            preparedStatement.setString(3, osType);
            preparedStatement.setInt(4, sessions);

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.err.println(TAG + "error when insert into os: " + e.getMessage());
        }
        return true;
    }

    public boolean insertIntoLocation(String appId, String location, int sessions) {
        String sql = "insert into location value (default, ?, ?, ?, ?)";
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, appId);
            preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            preparedStatement.setString(3, location);
            preparedStatement.setInt(4, sessions);

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.err.println(TAG + "error when insert into location: " + e.getMessage());
        }
        return true;
    }

    public boolean insertIntoDevice(String appId, String deviceType, int sessions) {
        String sql = "insert into device value (default, ?, ?, ?, ?)";
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, appId);
            preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            preparedStatement.setString(3, deviceType);
            preparedStatement.setInt(4, sessions);

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.err.println(TAG + "error when insert into device: " + e.getMessage());
        }
        return true;
    }

    public boolean insertIntoBrowser(String appId, String browserType, int sessions) {
        String sql = "insert into browser value (default, ?, ?, ?, ?)";
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, appId);
            preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            preparedStatement.setString(3, browserType);
            preparedStatement.setInt(4, sessions);

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.err.println(TAG + "error when insert into browser: " + e.getMessage());
        }
        return true;
    }

    public boolean insertIntoAppOverview(String appId, int sessions, int pageViews, int uniquePageViews,
            int newVisitor, int returnVisitor, int bounces, int entrances, int exits, long totalSessionDuration) {
        String sql = "insert into app_overview value (default, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, appId);
            preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            preparedStatement.setInt(3, sessions);
            preparedStatement.setInt(4, pageViews);
            preparedStatement.setInt(5, uniquePageViews);
            preparedStatement.setInt(6, newVisitor);
            preparedStatement.setInt(7, returnVisitor);
            preparedStatement.setInt(8, bounces);
            preparedStatement.setInt(9, entrances);
            preparedStatement.setInt(10, exits);
            preparedStatement.setLong(11, totalSessionDuration);

            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            System.err.println(TAG + "error when insert into app view: " + e.getMessage());
            return false;
        }
        return true;
    }
    
    public void insertToPageByFile(String path){
        String sql = "load data local infile '" + path +"' \n" +
                    " replace \n" +
                    " into table page_overview \n" +
                    " columns terminated by '\\t' \n" +
                    " (app_id,path,pageviews,unique_pageviews,bounces,entrances,exits,total_time_on_page)";
        try {
            preparedStatement = connection.prepareStatement(sql);
            
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        try {
//            ZDatabaseHelper dbh = new ZDatabaseHelper("118.102.6.118:3306", "root", "123456");
//            System.out.println(dbh.insertIntoReferal("app2", "google", 111, 13412));
//            dbh.close();
//        } catch (Exception ex) {
//            // TODO Auto-generated catch block
//            ex.printStackTrace();
//        }
//    }

}
