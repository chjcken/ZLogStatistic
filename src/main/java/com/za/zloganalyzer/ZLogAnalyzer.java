/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.zloganalyzer;

import com.za.zdatabasehelper.ZDatabaseHelper;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

/**
 *
 * @author datbt
 */
public class ZLogAnalyzer implements Serializable {

    private final String TAG = "[ZLogStatistic]\t";
    private final String MASTER = "local[2]";
    private final String LOG_TABLE = "logsTable";

    private static final ZLogAnalyzer instance = new ZLogAnalyzer();

    private final String PAGE_OVERVIEW_SQL = 
            "SELECT idsite,url,COUNT(*) as pageviews,COUNT(DISTINCT idvisit) as unique_pageviews,SUM(duration) as total_time from " + LOG_TABLE + " group by idsite,url,path_duration";
    private final String OS_SQL = 
            "SELECT idsite,os,COUNT(DISTINCT idvisit) as sessions from " + LOG_TABLE + " group by idsite,os";
    private final String BROWSER_SQL = 
            "SELECT idsite,us_br,COUNT(DISTINCT idvisit) as sessions from " + LOG_TABLE + " group by idsite,us_br";
    private final String LOCATION_SQL = 
            "SELECT idsite,ct_city,COUNT(DISTINCT idvisit) as sessions from " + LOG_TABLE + " group by idsite,ct_city";
    private final String DEVICE_SQL = 
            "SELECT idsite,device,COUNT(DISTINCT idvisit) as sessions from " + LOG_TABLE + " group by idsite,device";
    private final String REFERAL_SQL = 
            "SELECT idsite,urlref,ref_type,COUNT(DISTINCT idvisit) as sessions from " + LOG_TABLE + " group by idsite,urlref,ref_type";
    private final String APP_OVERVIEW_SQL = 
            "SELECT idsite,COUNT(DISTINCT idvisit) as sessions, COUNT(*) as pageviews from " + LOG_TABLE + "  group by idsite";

    private ZLogAnalyzer() {
    }

    public void analyze(String logSource) {
        if (logSource == null) {
            throw new NullPointerException("logSource is null.");
        }
        try {            
            
            SparkConf sparkConf;
            JavaSparkContext sparkContext;
            JavaSQLContext sqlContext;
            
            sparkConf = new SparkConf().
                    setMaster(MASTER).
                    setAppName(ZLogAnalyzer.class.getName()).
                    set("spark.executor.memory", "2g");
            sparkContext = new JavaSparkContext(sparkConf);
            sqlContext = new JavaSQLContext(sparkContext);
            
            JavaRDD<ZLogObject> accessLogs = sparkContext.textFile(logSource)
                    .map(new Function<String, ZLogObject>() {
                        @Override
                        public ZLogObject call(String line) throws Exception {
                            return ZLogObject.parseFromLogLine(line);
                        }
                    });

            JavaSchemaRDD schemaRDD = sqlContext.applySchema(accessLogs, ZLogObject.class);
            schemaRDD.registerTempTable(LOG_TABLE);
            sqlContext.sqlContext().cacheTable(LOG_TABLE);   

            //calculate pageOverview
            List<Row> pageOverviewTable = sqlContext.sql(PAGE_OVERVIEW_SQL).collect();//missing bound, entrance, exit
            
            //calculate os
            List<Row> osTable = sqlContext.sql(OS_SQL).collect();
            
            //calculate referal
            List<Row> referalTable = sqlContext.sql(REFERAL_SQL).collect();
            
            //calculate location
            List<Row> locationTable = sqlContext.sql(LOCATION_SQL).collect();
            
            //calculate device
            List<Row> deviceTable = sqlContext.sql(DEVICE_SQL).collect();
            
            //calculate browser
            List<Row> browserTable = sqlContext.sql(BROWSER_SQL).collect();
            
            //calculate app overview
            List<Row> appOverviewTable = sqlContext.sql(APP_OVERVIEW_SQL).collect();

            ZDatabaseHelper dbHelper = new ZDatabaseHelper("localhost", "root", "qwe123");
            
            HashMap<String, int[]> map = new HashMap<>();
            
            //insert to db
            
            for (Row row : pageOverviewTable) {
                
                String appId = row.getString(0);
                String path = row.getString(1);
                int pageViews = (int)row.getLong(2);
                int uniquePageViews = (int)row.getLong(3);
                int bounds = getBoundOfPage(appId, path, sqlContext);
                int entrances = getSessionStartOrExitWithPage(appId, path, sqlContext, false);
                int exits = getSessionStartOrExitWithPage(appId, path, sqlContext, true);
                long totalTimeonPage = row.getLong(4);
                
                if (!map.containsKey(appId)){
                    map.put(appId, new int[]{0, 0, 0, 0, 0});
                }
                
                map.put(appId, 
                        new int[]{map.get(appId)[0]+= uniquePageViews,
                            map.get(appId)[1] += bounds, map.get(appId)[2] += entrances,
                            map.get(appId)[3] += exits, map.get(appId)[4] += totalTimeonPage
                        });
                
                dbHelper.insertIntoPageOverview(appId, path, pageViews,
                        uniquePageViews, bounds, entrances, exits, totalTimeonPage);                                
            }
            
            for (Row row : referalTable) {
                String appId = row.getString(0);
                String urlRef = row.getString(1);
                int refType = row.getInt(2);
                int sessions = (int)row.getLong(3);
                dbHelper.insertIntoReferal(appId, urlRef, refType, sessions);
            }
            
            for (Row row : osTable){
                String appId = row.getString(0);
                String osType = row.getString(1);
                int sessions = (int)row.getLong(2);
                dbHelper.insertIntoOs(appId, osType, sessions);
            }
            
            for (Row row : deviceTable){
                String appId = row.getString(0);
                String device = row.getString(1);
                int sessions = (int)row.getLong(2);
                dbHelper.insertIntoDevice(appId, device, sessions);
            }
            
            for (Row row : locationTable){
                String appId = row.getString(0);
                String location = row.getString(1);
                int sessions = (int)row.getLong(2);
                dbHelper.insertIntoLocation(appId, location, sessions);
            }
            
            for (Row row : browserTable){
                String appId = row.getString(0);
                String browser = row.getString(1);
                int sessions = (int)row.getLong(2);
                dbHelper.insertIntoBrowser(appId, browser, sessions);
            }
            
            for (Row row : appOverviewTable){
                String appId = row.getString(0);
                int sessions = (int)row.getLong(1);
                int pageviews = (int)row.getLong(2);
                int uniquePageView = map.get(appId)[0];
                
                int[] arrVisitor = new int[3];
                
                getNewAndReturnVisitor(appId, sqlContext, arrVisitor);
                
                //int total = arrVisitor[0];
                int newVisitor = arrVisitor[2];
                int returnVisitor = arrVisitor[1];
                
                int bounds = map.get(appId)[1];
                int entrances = map.get(appId)[2];
                int exits = map.get(appId)[3];
                long totalSessionDuration = map.get(appId)[4];
                
                dbHelper.insertIntoAppOverview(appId, sessions, pageviews, uniquePageView, newVisitor, returnVisitor, bounds, entrances, exits, totalSessionDuration);
            }
            
            dbHelper.close();
            sparkContext.stop();
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println(TAG + "error while parsing log: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void getNewAndReturnVisitor(String idSite, JavaSQLContext sqlContext, int[] arr) {
        //Total Visitor ,     //Return visitor ,    //New Visitor 
        String sqlTotal = "Select distinct id from " + LOG_TABLE + " where idsite='" + idSite + "'";
        int totalVS = sqlContext.sql(sqlTotal).collect().size();
        String sqlParent = "Select id from " + LOG_TABLE + " where idsite='" + idSite + "' group by id having min(new_visitor)=0";
        int numResturn = sqlContext.sql(sqlParent).collect().size();
        int numNew = totalVS - numResturn;
        arr[0] = totalVS;
        arr[1] = numResturn;// return
        arr[2] = numNew;// new
    }

    private int getBoundOfPage(String idSite, String page, JavaSQLContext sqlContext) {
        int bounce;
        String sql = "SELECT idvisit from " + LOG_TABLE + " where idsite='" + idSite + "' and url='" + page + "' group by idvisit having COUNT(*)=1";
        bounce = sqlContext.sql(sql).collect().size();
        return bounce;
    }

    private int getSessionStartOrExitWithPage(String idSite, String page, JavaSQLContext sqlContext, boolean isExits) {
        int countSession = 0;
        String sqlParent = "Select distinct idvisit from " + LOG_TABLE + " tk where tk.idsite='" + idSite + "' and tk.url='" + page + "'";
        List<Row> rowParents = sqlContext.sql(sqlParent).collect();
        for (Row rowParent : rowParents) {
            String sql = "select url from " + LOG_TABLE + " tk1 where tk1.idvisit='" + rowParent.get(0) + "' limit 1";
            if (isExits) {
                sql = "select url from " + LOG_TABLE + " tk1 where tk1.idvisit='" + rowParent.get(0) + "' order by idtscr desc limit 1";
            }
            Row r = sqlContext.sql(sql).collect().get(0);
            if (r.getString(0) != null && r.getString(0).equals(page)) {
                countSession++;
            }
        }
        return countSession;
    }
    
    public static ZLogAnalyzer getInstance() {
        return instance;
    }

    public static void main(String... args) {
        String path = "ZAlog";
        ZLogAnalyzer.getInstance().analyze(path);
    }
}
