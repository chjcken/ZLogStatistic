/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.zloganalyzer;

import com.za.zdatabasehelper.ZDatabaseHelper;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


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
            "SELECT idsite,url,COUNT(*) as pageviews,COUNT(DISTINCT idvisit) as unique_pageviews from " + LOG_TABLE + " group by idsite,url";
    private final String TIME_ON_PAGE_SQL = 
            "SELECT idsite,path_duration,SUM(duration) as total_time from logsTable group by idsite,path_duration";           
    private final String BOUNDS_SQL = 
            "SELECT idsite,url,idvisit from " + LOG_TABLE + " group by idsite,url,idvisit having COUNT(*)=1";
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
            SQLContext sqlContext;
            
            sparkConf = new SparkConf().
                    setMaster(MASTER).
                    setAppName(ZLogAnalyzer.class.getName()).
                    set("spark.executor.memory", "2g");
            sparkContext = new JavaSparkContext(sparkConf);
            sqlContext = new SQLContext(sparkContext);
            
            JavaRDD<ZLogObject> accessLogs = sparkContext.textFile(logSource)
                    .map(new Function<String, ZLogObject>() {
                        @Override
                        public ZLogObject call(String line) throws Exception {
                            return ZLogObject.parseFromLogLine(line);
                        }
                    });

            DataFrame dataFrame = sqlContext.createDataFrame(accessLogs, ZLogObject.class);
            dataFrame.registerTempTable(LOG_TABLE);
            sqlContext.cacheTable(LOG_TABLE);    
            

            //calculate pageOverview
            List<Row> pageOverviewTable = sqlContext.sql(PAGE_OVERVIEW_SQL).collectAsList();//missing bound, entrance, exit, timeonpage
            
            List<Row> boundTable = sqlContext.sql(BOUNDS_SQL).collectAsList();
            List<Object[]> listEntrances = getListEntranceOrExit(sqlContext, false);//for calculating entrances
            List<Object[]> listExits = getListEntranceOrExit(sqlContext, true);//for calculatiing exits
            List<Row> timeOnPageTable = sqlContext.sql(TIME_ON_PAGE_SQL).collectAsList();//for calculate time on page
            
            
            //calculate os
            List<Row> osTable = sqlContext.sql(OS_SQL).collectAsList();
            
            //calculate referal
            List<Row> referalTable = sqlContext.sql(REFERAL_SQL).collectAsList();
            
            //calculate location
            List<Row> locationTable = sqlContext.sql(LOCATION_SQL).collectAsList();
            
            //calculate device
            List<Row> deviceTable = sqlContext.sql(DEVICE_SQL).collectAsList();
            
            //calculate browser
            List<Row> browserTable = sqlContext.sql(BROWSER_SQL).collectAsList();
            
            //calculate app overview
            List<Row> appOverviewTable = sqlContext.sql(APP_OVERVIEW_SQL).collectAsList();//missing unique_pageview, entrances, exit, new,return visitor, total session duration
            List<Object[]> listNewAndReturn = getListNewAndReturnVisitor(sqlContext);//for calculate new/return visitor
            HashMap<String, long[]> map = new HashMap<>();//for calculate bound, entrance, exit, total duration from page view
            
            
            ZDatabaseHelper dbHelper = new ZDatabaseHelper("localhost", "root", "qwe123"); 
            
            //insert to db            
            for (Row row : pageOverviewTable) {                
                String appId = row.getString(0);
                String path = row.getString(1);
                int pageViews = (int)row.getLong(2);
                int uniquePageViews = (int)row.getLong(3);
                int bounds = getBoundOfPage(appId, path, boundTable);
                int entrances = getEntrancesOrExits(appId, path, listEntrances);
                int exits = getEntrancesOrExits(appId, path, listExits);
                long totalTimeonPage = getTimeOnPage(appId, path, timeOnPageTable);
                

                if (!map.containsKey(appId)){
                    map.put(appId, new long[]{0, 0, 0, 0, 0});
                }
                
                map.put(appId, new long[]{
                    map.get(appId)[0]+= uniquePageViews, 
                    map.get(appId)[1] += bounds, map.get(appId)[2] += entrances,
                    map.get(appId)[3] += exits, map.get(appId)[4] += totalTimeonPage});
                
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
                int uniquePageView = (int)map.get(appId)[0]; 
                
                Object[] newAndReturn = getNewAndReturnVisitor(appId, listNewAndReturn);
                int newVisitor = (int)((long)newAndReturn[1]);
                int returnVisitor = (int)((long)newAndReturn[2]);
                
                int bounds = (int)map.get(appId)[1];
                int entrances = (int)map.get(appId)[2];
                int exits = (int)map.get(appId)[3];
                long totalSessionDuration = map.get(appId)[4];
                
                dbHelper.insertIntoAppOverview(appId, sessions, pageviews, 
                        uniquePageView, newVisitor, returnVisitor, bounds, 
                        entrances, exits, totalSessionDuration);
            }
            
            dbHelper.close();
            sparkContext.stop();
        } catch (Exception e) {
            System.err.println(TAG + "error while parsing log: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private long getTimeOnPage(String appId, String path, List<Row> timeOnPageTable){
        for (Row row : timeOnPageTable){
            if (appId.equals(row.getString(0)) && path.equals(row.getString(1))){                
                    return row.getLong(2);
            }
        }        
        return 0;
    } 
    
    private List<Object[]> getListNewAndReturnVisitor(SQLContext sqlContext){
        String totalVisitorSQL = "Select idsite,count(distinct id) as total_visitor from " + LOG_TABLE + " group by idsite";
        List<Row> listTotalVisitor = sqlContext.sql(totalVisitorSQL).collectAsList();
        String returnVisitorSQL = "Select idsite,count(id) as return_visitor from (Select idsite,id from " + LOG_TABLE + " group by idsite,id having min(new_visitor)=0) as tbl group by idsite";
        List<Row> listReturnVisitor = sqlContext.sql(returnVisitorSQL).collectAsList();
        List<Object[]> listNewAndReturn = new ArrayList<>();
        for (Row rowTotal : listTotalVisitor){
            String appId = rowTotal.getString(0);
            for (Row rowReturn : listReturnVisitor)
                if (appId.equals(rowReturn.getString(0))){
                    long returnVisitor = rowReturn.getLong(1);
                    long total = rowTotal.getLong(1);
                    long newVisitor =  total - returnVisitor;
                    //System.out.println("{TAG}\t" + appId +"\t"+ total + "\t" + newVisitor + "\t" + returnVisitor);
                    listNewAndReturn.add(
                            new Object[]{appId, newVisitor, returnVisitor});                    
                }
        }
        return listNewAndReturn;
    }
    
    private Object[] getNewAndReturnVisitor(String appId, List<Object[]> listNewAndReturn){
        for (Object[] object : listNewAndReturn){
            if (appId.equals(object[0]))
                return object;
        }
        return null;
    }
    
    private int getBoundOfPage(String appId, String path, List<Row> boundTable){
        int boundCount = 0;
        for (Row row : boundTable){
            if (appId.equals(row.getString(0)) && path.equals(row.getString(1)))
                boundCount++;
        }        
        return boundCount;
    }

    private int getSessionStartOrExitWithPage(String idSite, String page, SQLContext sqlContext, boolean isExits) {
        int countSession = 0;
        String sqlParent = "Select distinct idvisit from " + LOG_TABLE + " tk where tk.idsite='" + idSite + "' and tk.url='" + page + "'";
        List<Row> rowParents = sqlContext.sql(sqlParent).collectAsList();
        for (Row rowParent : rowParents) {
            String sql = "select url from " + LOG_TABLE + " tk1 where tk1.idvisit='" + rowParent.get(0) + "' limit 1";
            if (isExits) {
                sql = "select url from " + LOG_TABLE + " tk1 where tk1.idvisit='" + rowParent.get(0) + "' order by idtscr desc limit 1";
            }
            Row r = sqlContext.sql(sql).collectAsList().get(0);
            if (r.getString(0).equals(page)) {
                countSession++;
            }
        }
        return countSession;
    }
    
    private List<Object[]> getListEntranceOrExit(SQLContext sqlContext, boolean isExits) {
        List<Object[]> listData = new ArrayList<>();
        String entrancesSQL = "SELECT idsite,url from logsTable group by idsite,url";
        List<Row> rows = sqlContext.sql(entrancesSQL).collectAsList();
        for (Row row : rows) {
            String idSite = row.getString(0);
            String url = row.getString(1);
            int count = getSessionStartOrExitWithPage(idSite, url, sqlContext, isExits);
            listData.add(new Object[]{idSite, url, count});
        }
        return listData;
    }
    
    private int getEntrancesOrExits(String appId, String path, List<Object[]> listEntranceOrExit){
        for (Object[] object : listEntranceOrExit){
            if (appId.equals(object[0]) && path.equals(object[1]))
                return (int)object[2];
        }
        return 0;
    }
    
    public static ZLogAnalyzer getInstance() {
        return instance;
    }

    public static void main(String... args) {
        String path = "ZAlog";
        ZLogAnalyzer.getInstance().analyze(path);
    }
}
