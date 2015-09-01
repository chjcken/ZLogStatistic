/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.zloganalyzer;

import com.za.zobject.TestObj;
import com.za.zobject.ZPageObj;
import com.za.zobject.ZAppTotalVisitorObj;
import com.za.zobject.ZAppUniquePageviewObj;
import com.za.zobject.ZOSObj;
import com.za.zobject.ZPageEntranceObj;
import com.za.zobject.ZAppBounceObj;
import com.za.zobject.ZPageTimeObj;
import com.za.zobject.ZDeviceObj;
import com.za.zobject.ZLogObject;
import com.za.zobject.ZAppTimeObj;
import com.za.zobject.ZLanguageObj;
import com.za.zobject.ZAppExitObj;
import com.za.zobject.ZPageExitObj;
import com.za.zobject.ZAppObj;
import com.za.zobject.ZLocationObj;
import com.za.zobject.ZAppReturnVisitorObj;
import com.za.zobject.ZAppEntranceObj;
import com.za.zobject.ZAppReturnVisitorTempObj;
import com.za.zobject.ZPageBounceObj;
import com.za.zobject.ZBrowserObj;
import com.za.zobject.ZObject;
import com.za.zobject.ZPageBounceTempObj;
import com.za.zobject.ZPageEntranceAndExitTempObj;
import com.za.zobject.ZReferalObj;
import static com.za.zutil.ZSparkSQL.*;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.storage.StorageLevel;

/**
 *
 * @author datbt
 */
public class ZLogAnalyzer implements Serializable {

    private final String TAG = "[ZLogStatistic]\t";
    private final String MASTER = "local[2]";
    private final String LOG_TABLE = "logsTable";
    
    private boolean analyzeDone = false;

    private static final ZLogAnalyzer instance = new ZLogAnalyzer();

    private final String PAGE_OVERVIEW_SQL
            = "SELECT app_id,path,COUNT(*) as pageviews,COUNT(DISTINCT idvisit) as unique_pageviews from " + LOG_TABLE + " group by app_id,path";
    private final String OS_SQL
            = "SELECT app_id,os_type,COUNT(DISTINCT idvisit) as sessions from " + LOG_TABLE + " group by app_id,os_type";
    private final String BROWSER_SQL
            = "SELECT app_id,browser_type,COUNT(DISTINCT idvisit) as sessions from " + LOG_TABLE + " group by app_id,browser_type";
    private final String LOCATION_SQL
            = "SELECT app_id,location,COUNT(DISTINCT idvisit) as sessions from " + LOG_TABLE + " group by app_id,location";
    private final String DEVICE_SQL
            = "SELECT app_id,device_type,COUNT(DISTINCT idvisit) as sessions from " + LOG_TABLE + " group by app_id,device_type";
    private final String LANGUAGE_SQL
            = "SELECT app_id,us_lang,COUNT(DISTINCT idvisit) as sessions from " + LOG_TABLE + " group by app_id,us_lang";

    private final String REFERAL_SQL
            = "SELECT app_id,url_ref,ref_type,COUNT(DISTINCT idvisit) as sessions from " + LOG_TABLE + " group by app_id,url_ref,ref_type";
    private final String APP_OVERVIEW_SQL
            = "SELECT app_id,COUNT(DISTINCT idvisit) as sessions, COUNT(*) as pageviews from " + LOG_TABLE + "  group by app_id";

    private ZLogAnalyzer() {
    }
    
    private JavaSparkContext init(){
        
        SparkConf sparkConf;
        JavaSparkContext sparkContext;
        sparkConf = new SparkConf()
                .setMaster(MASTER)
                .setAppName(ZLogAnalyzer.class.getName())
                .set("spark.executor.memory", "2g")
                .set("spark.storage.memoryFraction", "0.4");

        sparkContext = new JavaSparkContext(sparkConf);
        return sparkContext;        
    }

    public void analyze(String logSource, String tempFolder) {
        if (logSource == null) {
            throw new NullPointerException("logSource is null.");
        }      
        
        try {

            SparkConf sparkConf;
            JavaSparkContext sparkContext;
            SQLContext sqlContext;
            sparkConf = new SparkConf()
                    .setMaster(MASTER)
                    .setAppName(ZLogAnalyzer.class.getName())
                    .set("spark.executor.memory", "2g")
                    .set("spark.storage.memoryFraction", "0.4");

            sparkContext = new JavaSparkContext(sparkConf);
            sqlContext = new SQLContext(sparkContext);

            JavaRDD<ZLogObject> accessLogs = sparkContext.textFile(logSource)
                    .map(new Function<String, ZLogObject>() {
                        @Override
                        public ZLogObject call(String line) throws Exception {
                            return ZLogObject.parseFromLogLine(line);
                        }
                    });

            DataFrame dataFrame = 
                    sqlContext.createDataFrame(accessLogs, ZLogObject.class)
                    .persist(StorageLevel.MEMORY_AND_DISK_SER_2());
            dataFrame.registerTempTable(LOG_TABLE);

            
//            DataFrame df = dataFrame.select("idsite", "url").dropDuplicates();  
//            String sql = "select idsite, idvisit, min(idtscr) as idtscr1 from logsTable group by idsite, idvisit";
//            DataFrame df2 = sqlContext.sql(sql);//dataFrame.groupBy("idsite","idvisit").min("idtscr");
//            df2.registerTempTable("temp");
//            String sql1 = "select tk.idsite,tk.url,count(tk.idvisit) as entrance " +
//                        "from (select idvisit,min(idtscr) as mintime " +
//                        "from logsTable group by idvisit) as abc , logsTable tk " +
//                        "where abc.idvisit=tk.idvisit " +
//                        "and abc.mintime=tk.idtscr " +
//                        "group by tk.idsite,tk.url";
//            DataFrame df3 = sqlContext.sql(sql1);
//            String pageOverviewSql = "select 0,tk.app_id,tk.path,'"+new Timestamp(System.currentTimeMillis())+"' as date_tracking, tblPage.pageviews,tblPage.unique_pageviews,coalesce(tblBounce.bounces, 0) as bounces,coalesce(tblEntrances.entrances, 0) as entrances,coalesce(tblExits.exits, 0) as exits,coalesce(tblDuration.total_time_on_page, 0) as total_time_on_page from (select tk1.app_id , tk1.path from logsTable tk1 group by tk1.app_id,tk1.path) as tk left join (\n"
//                    + "SELECT tk.app_id,tk.path,COUNT(tk.idvisit) as bounces from (SELECT idvisit from logsTable group by idvisit having COUNT(*)=1) as tbl, logsTable tk\n"
//                    + "where tk.idvisit = tbl.idvisit\n"
//                    + "group by tk.app_id,tk.path) as tblBounce on tblBounce.app_id=tk.app_id and tblBounce.path=tk.path\n"
//                    + "\n"
//                    + "left join (select tk.app_id,tk.path,count(tk.idvisit) as entrances\n"
//                    + "from (select idvisit,min(idtscr) as mintime\n"
//                    + "from logsTable group by idvisit) as abc, logsTable tk\n"
//                    + "where abc.idvisit=tk.idvisit\n"
//                    + "and abc.mintime=tk.idtscr\n"
//                    + "group by tk.app_id,tk.path) as tblEntrances on tblEntrances.app_id=tk.app_id and tblEntrances.path=tk.path\n"
//                    + "\n"
//                    + "left join (select tk.app_id,tk.path,count(tk.idvisit) as exits\n"
//                    + "from (select idvisit,max(idtscr) as mintime\n"
//                    + "from logsTable group by idvisit) as abc, logsTable tk\n"
//                    + "where abc.idvisit=tk.idvisit\n"
//                    + "and abc.mintime=tk.idtscr\n"
//                    + "group by tk.app_id,tk.path) as tblExits on tblExits.app_id=tk.app_id and tblExits.path=tk.path\n"
//                    + "\n"
//                    + "left join (SELECT app_id,path_duration,SUM(duration) as total_time_on_page from logsTable group by app_id,path_duration) as tblDuration on tblDuration.app_id=tk.app_id and tblDuration.path_duration=tk.path\n"
//                    + "\n"
//                    + "left join (SELECT app_id,path,COUNT(*) as pageviews,COUNT(DISTINCT idvisit) as unique_pageviews from logsTable group by app_id,path)\n"
//                    + "as tblPage on tblPage.app_id=tk.app_id and tblPage.path=tk.path";
//
////            DataFrame df = sqlContext.sql(pageOverviewSql);
////            
//            String appOverviewSql = "select 0,app.app_id,'"+new Timestamp(System.currentTimeMillis())+"' as date_tracking,app.sessions,app.pageviews,tblTemp.unique_pageviews,(tblTotal.total_visitor - coalesce(tblReturn.return_visitor,0)) as new_visitors,coalesce(tblReturn.return_visitor,0) as return_visitors,tblTemp.bounces,tblTemp.entrances,tblTemp.exits,\n" +
//                    "tblTemp.total_time_on_page as total_session_duration\n" +
//                    "from (SELECT app_id,COUNT(DISTINCT idvisit) as sessions, COUNT(*) as pageviews from logsTable group by app_id) as app\n" +
//                    "left join (Select app_id,count(distinct id) as total_visitor from logsTable group by app_id) as tblTotal on app.app_id = tblTotal.app_id\n" +
//                    "left join (Select app_id,count(id) as return_visitor from (Select app_id,id from logsTable group by app_id,id having min(new_visitor)=0) as tbl group by app_id) as tblReturn on app.app_id = tblReturn.app_id " +
//                    "left join (select app_id, sum(unique_pageviews) as unique_pageviews, sum(bounces) as bounces, sum(entrances) as entrances, sum(exits) as exits, sum(total_time_on_page) as total_time_on_page from temp group by app_id) as tblTemp on app.app_id=tblTemp.app_id";
//            
////
////            //calculate pageOverview
//            DataFrame pageOverviewTable = sqlContext.sql(pageOverviewSql);
//            pageOverviewTable.registerTempTable("temp");
            //calculate os
//            DataFrame osTable = sqlContext.sql(OS_SQL);
            //  String path;
//            String url = "jdbc:mysql://localhost/zanalytics";
//            String table = "page_overview";
//            Properties prop = new Properties();
//            prop.setProperty("user", "root");
//            prop.setProperty("password", "qwe123");
//            
//            pageOverviewTable.write().mode(SaveMode.Append).jdbc(url, table, prop);
//            DataFrame appOverviewTable = sqlContext.sql(appOverviewSql);
//            appOverviewTable.write().mode(SaveMode.Append).jdbc(url, "app_overview", prop);
//            pageOverviewTable.toJavaRDD().map(new Function<Row, String>() {
//
//                @Override
//                public String call(Row row) {
//                    int i = 0;
//                    String delimiter = "\t";
//
//                    try {
//                        StringBuilder result = new StringBuilder();
//                        result.append(row.getString(i++)).append(delimiter);
//                        result.append(row.getString(i++)).append(delimiter);
//                        result.append(row.getLong(i++)).append(delimiter);
//                        result.append(row.getLong(i++)).append(delimiter);
//                        result.append(row.getLong(i++)).append(delimiter);
//                        result.append(row.getLong(i++)).append(delimiter);
//                        result.append(row.getLong(i++)).append(delimiter);
//                        result.append(row.getLong(i++)).append(delimiter);
//                        return result.toString();
//                    } catch (Exception ex) {
//                        return "";
//                    }
//                }
//
//            }).repartition(1).saveAsTextFile(String.valueOf(System.currentTimeMillis()));
//            
//            //calculate referal
//            DataFrame referalTable = sqlContext.sql(REFERAL_SQL);
//            
//            //calculate location
//            DataFrame locationTable = sqlContext.sql(LOCATION_SQL);
//            
//            //calculate device
//            DataFrame deviceTable = sqlContext.sql(DEVICE_SQL);
//            
//            //calculate browser
//            DataFrame browserTable = sqlContext.sql(BROWSER_SQL);
//            
            //calculate app overview
            //DataFrame appOverviewTable = sqlContext.sql(appOverviewSql);//missing unique_pageview, entrances, exit,total session duration
            //pageOverviewTable.show();
            String pageSQL
                    = "select app_id,path,COUNT(*) as pageviews,COUNT(DISTINCT idvisit) as unique_pageviews from logsTable group by app_id,path";

            String pageBounceTempSQL 
                    = "select idvisit from logsTable group by idvisit having COUNT(*)=1";
            
            String pageEntranceAndExitTempSQL
                    = "select idvisit,min(idtscr) as mintime, max(idtscr) as maxtime from logsTable group by idvisit";
            
            
//            String pageBoundSQL
//                    = "select tk.app_id,tk.path,COUNT(tk.idvisit) as bounces from pagebouncetemp as tbl, logsTable tk where tk.idvisit = tbl.idvisit group by tk.app_id,tk.path";
//
//            String pageEntranceSQL
//                    = "select tk.app_id,tk.path,count(tk.idvisit) as entrances from pageentranceandexittemp as temp, logsTable tk where temp.idvisit=tk.idvisit and temp.mintime=tk.idtscr group by tk.app_id,tk.path";
//
//            String pageExitSQL
//                    = "select tk.app_id,tk.path,count(tk.idvisit) as exits from pageentranceandexittemp as temp, logsTable tk where temp.idvisit=tk.idvisit and temp.maxtime=tk.idtscr group by tk.app_id,tk.path";
            

            
            
            
            String pageTotalTimeSQL
                    = "select app_id,path_duration,SUM(duration) as total_time_on_page from logsTable group by app_id,path_duration";

            String appSQL
                    = "SELECT app_id,COUNT(DISTINCT idvisit) as sessions, COUNT(*) as pageviews from " + LOG_TABLE + "  group by app_id";

            String appTotalVisitorSQL
                    = "Select app_id,count(distinct id) as total_visitor from logsTable group by app_id";

            
            String appReturnVisitorTempSQL
                    = "Select app_id,id from logsTable group by app_id,id having min(new_visitor)=0";
            
//            String appReturnVisitorSQL
//                    = "Select app_id,count(id) as return_visitor from appreturnvisitortemp as tlbTmp group by app_id";


            DataFrame page = sqlContext.sql(PAGE_SQL);
            
            DataFrame pageBouncesTemp = sqlContext.sql(PAGE_BOUNCE_TEMP_SQL);
            //pageBouncesTemp.registerTempTable("pagebouncetemp");
            DataFrame pageEntranceAndExitTemp = sqlContext.sql(pageEntranceAndExitTempSQL).persist(StorageLevel.MEMORY_AND_DISK_SER_2());
            //pageEntranceAndExitTemp.registerTempTable("pageentranceandexittemp");
            
//            DataFrame pageBounds = sqlContext.sql(pageBoundSQL);
//            DataFrame pageEntrances = sqlContext.sql(pageEntranceSQL);
//            DataFrame pageExits = sqlContext.sql(pageExitSQL);
            
            
            
            DataFrame pageTotalTime = sqlContext.sql(pageTotalTimeSQL);

            DataFrame app = sqlContext.sql(appSQL);
            DataFrame appUniquePageviews = page.groupBy("app_id").sum("unique_pageviews");
            DataFrame appTotalVisitor = sqlContext.sql(appTotalVisitorSQL);
            
            DataFrame appReturnVisitorTemp = sqlContext.sql(appReturnVisitorTempSQL);
            //appReturnVisitorTemp.registerTempTable("appreturnvisitortemp");
            
//            DataFrame appReturnVisitor = sqlContext.sql(appReturnVisitorSQL);
//            DataFrame appBounds = pageBounds.groupBy("app_id").sum("bounces");
//            DataFrame appEntrances = pageEntrances.groupBy("app_id").sum("entrances");
//            DataFrame appExits = pageExits.groupBy("app_id").sum("exits");
            DataFrame appTime = pageTotalTime.groupBy("app_id").sum("total_time_on_page");

            DataFrame os = sqlContext.sql(OS_SQL);

            DataFrame location = sqlContext.sql(LOCATION_SQL);

            DataFrame device = sqlContext.sql(DEVICE_SQL);

            DataFrame browser = sqlContext.sql(BROWSER_SQL);

            DataFrame language = sqlContext.sql(LANGUAGE_SQL);

            DataFrame referal = sqlContext.sql(REFERAL_SQL);
            
            

            /*
             * save page figure to temp file
             */
            page.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row row) {
                    StringBuilder result = new StringBuilder();
                    result.append(row.getString(0)).append(" ");
                    result.append(row.getString(1)).append(" ");
                    result.append(row.getLong(2)).append(" ");
                    result.append(row.getLong(3));
                    return result.toString();
                }
            }).saveAsTextFile(tempFolder + "page");
            
            
            pageBouncesTemp.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row row) {
                    StringBuilder result = new StringBuilder();
                    result.append(row.getString(0));
                    return result.toString();
                }
            }).saveAsTextFile(tempFolder + "pagebouncetemp");
            
            pageEntranceAndExitTemp.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row row) {
                    StringBuilder result = new StringBuilder();
                    result.append(row.getString(0)).append(" ");
                    result.append(row.getLong(1)).append(" ");
                    result.append(row.getLong(2));
                    return result.toString();
                }
            }).saveAsTextFile(tempFolder + "pageentranceandexittemp");

//            pageBounds.toJavaRDD().map(new Function<Row, String>() {
//
//                @Override
//                public String call(Row t1) {
//                    return pageFormat(t1);
//                }
//            }).saveAsTextFile(tempFolder + "pagebounces");
//
//            pageEntrances.toJavaRDD().map(new Function<Row, String>() {
//
//                @Override
//                public String call(Row t1) {
//                    return pageFormat(t1);
//                }
//            }).saveAsTextFile(tempFolder + "pageentrances");
//
//            pageExits.toJavaRDD().map(new Function<Row, String>() {
//
//                @Override
//                public String call(Row t1) {
//                    return pageFormat(t1);
//                }
//            }).saveAsTextFile(tempFolder + "pageexits");

            pageEntranceAndExitTemp.unpersist();
            
            pageTotalTime.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return pageFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "pagetotaltimes");

            /*
             * save app figure to temp file
             */
            app.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row row) {
                    StringBuilder result = new StringBuilder();
                    result.append(row.getString(0)).append(" ");
                    result.append(row.getLong(1)).append(" ");
                    result.append(row.getLong(2));
                    return result.toString();
                }
            }).saveAsTextFile(tempFolder + "app");
            
            
            
            appReturnVisitorTemp.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row row) {
                    StringBuilder result = new StringBuilder();
                    result.append(row.getString(0)).append(" ");
                    result.append(row.getString(1));
                    return result.toString();
                }
            }).saveAsTextFile(tempFolder + "appreturnvisitortemp");

//            appReturnVisitor.toJavaRDD().map(new Function<Row, String>() {
//
//                @Override
//                public String call(Row t1) {
//                    return appTotalFormat(t1);
//                }
//            }).saveAsTextFile(tempFolder + "appreturnvisitor");

            appTotalVisitor.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "apptotalvisitor");

            appUniquePageviews.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "appuniquepageview");

//            appBounds.toJavaRDD().map(new Function<Row, String>() {
//
//                @Override
//                public String call(Row t1) {
//                    return appTotalFormat(t1);
//                }
//            }).saveAsTextFile(tempFolder + "appbounce");
//
//        
//            
//            appEntrances.toJavaRDD().map(new Function<Row, String>() {
//
//                @Override
//                public String call(Row t1) {
//                    return appTotalFormat(t1);
//                }
//            }).saveAsTextFile(tempFolder + "appentrances");
//
//            appExits.toJavaRDD().map(new Function<Row, String>() {
//
//                @Override
//                public String call(Row t1) {
//                    return appTotalFormat(t1);
//                }
//            }).saveAsTextFile(tempFolder + "appexits");

            appTime.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "apptime");

            /*
             * save other to temp
             */
            os.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) throws Exception {
                    return otherFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "os");

            device.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) throws Exception {
                    return otherFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "device");

            browser.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) throws Exception {
                    return otherFormat(t1);
                }
            }).repartition(1).saveAsTextFile(tempFolder + "browser");

            location.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) throws Exception {
                    return otherFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "location");

            language.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) throws Exception {
                    return otherFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "language");

            referal.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row row) throws Exception {
                    StringBuilder result = new StringBuilder();
                    result.append(row.getString(0)).append(" ");
                    result.append(row.getString(1)).append(" ");
                    result.append(row.getInt(2)).append(" ");
                    result.append(row.getLong(3));
                    return result.toString();
                }
            }).saveAsTextFile(tempFolder + "referal");
            
            
            
            
            analyzePhase2(sparkContext, sqlContext, tempFolder);

            
            dataFrame.unpersist();
            

            long time = System.currentTimeMillis() - sparkContext.startTime();

            
            //sparkContext.close();
            sparkContext.stop();            
            
            System.err.println(TAG + "analyze raw log time: " + new SimpleDateFormat("hh:mm:ss").format(new Date(time)));

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
    
    
    public void analyzePhase2(JavaSparkContext sparkContext, SQLContext sqlContext, String tempFolder){
        try {
            JavaRDD<ZPageBounceTempObj> pageBounceTempRdd = sparkContext.textFile(tempFolder+"pagebouncetemp")
                    .map(new Function<String, ZPageBounceTempObj>() {

                        @Override
                        public ZPageBounceTempObj call(String t1) {
                            return ZPageBounceTempObj.parseFromLogLine(t1);
                        }
                    });
            JavaRDD<ZPageEntranceAndExitTempObj> pageEntranceAndExitTempRdd = sparkContext.textFile(tempFolder+"pageentranceandexittemp")
                    .map(new Function<String, ZPageEntranceAndExitTempObj>() {

                        @Override
                        public ZPageEntranceAndExitTempObj call(String t1) {
                            return ZPageEntranceAndExitTempObj.parseFromLogLine(t1);
                        }
                    });
            
            JavaRDD<ZAppReturnVisitorTempObj> appReturnVisitorTempRdd = sparkContext.textFile(tempFolder+"appreturnvisitortemp")
                    .map(new Function<String, ZAppReturnVisitorTempObj>() {

                        @Override
                        public ZAppReturnVisitorTempObj call(String t1) {
                            return ZAppReturnVisitorTempObj.parseFromLogLine(t1);
                        }
                    });
            sqlContext.createDataFrame(pageBounceTempRdd, ZPageBounceTempObj.class).registerTempTable("pagebouncetemp");
            sqlContext.createDataFrame(pageEntranceAndExitTempRdd, ZPageEntranceAndExitTempObj.class).registerTempTable("pageentranceandexittemp");
            sqlContext.createDataFrame(appReturnVisitorTempRdd, ZAppReturnVisitorTempObj.class).registerTempTable("appreturnvisitortemp");
            
            
            String pageBoundSQL
                    = "select tk.app_id,tk.path,COUNT(tk.idvisit) as bounces from pagebouncetemp as tbl, logsTable tk where tk.idvisit = tbl.idvisit group by tk.app_id,tk.path";

            String pageEntranceSQL
                    = "select tk.app_id,tk.path,count(tk.idvisit) as entrances from pageentranceandexittemp as temp, logsTable tk where temp.idvisit=tk.idvisit and temp.mintime=tk.idtscr group by tk.app_id,tk.path";

            String pageExitSQL
                    = "select tk.app_id,tk.path,count(tk.idvisit) as exits from pageentranceandexittemp as temp, logsTable tk where temp.idvisit=tk.idvisit and temp.maxtime=tk.idtscr group by tk.app_id,tk.path";
            
            String appReturnVisitorSQL
                    = "Select app_id,count(id) as return_visitor from appreturnvisitortemp as tlbTmp group by app_id";
            
            DataFrame pageBounds = sqlContext.sql(pageBoundSQL);
            DataFrame pageEntrances = sqlContext.sql(pageEntranceSQL);
            DataFrame pageExits = sqlContext.sql(pageExitSQL);
            
            DataFrame appReturnVisitor = sqlContext.sql(appReturnVisitorSQL);
            DataFrame appBounds = pageBounds.groupBy("app_id").sum("bounces");
            DataFrame appEntrances = pageEntrances.groupBy("app_id").sum("entrances");
            DataFrame appExits = pageExits.groupBy("app_id").sum("exits");
            
            
            //save page
            pageBounds.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return pageFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "pagebounces");

            pageEntrances.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return pageFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "pageentrances");

            pageExits.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return pageFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "pageexits");
            
            //save app
            appReturnVisitor.toJavaRDD().map(new Function<Row, String>() {
//
                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "appreturnvisitor");
            
            
            appBounds.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "appbounce");

        
            
            appEntrances.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "appentrances");

            appExits.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + "appexits");            
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }   

    public void joinTempFileAndWriteToDB(String tmpFolder) {
        try {
            SparkConf sparkConf;
            JavaSparkContext sparkContext;
            SQLContext sqlContext;

            sparkConf = new SparkConf()
                    .setMaster(MASTER)
                    .setAppName(ZLogAnalyzer.class.getName())
                    .set("spark.executor.memory", "1g");
            sparkContext = new JavaSparkContext(sparkConf);
            sqlContext = new SQLContext(sparkContext);

            /*
             *parse tmp file to page table
             */
            JavaRDD<ZPageObj> pageRdd = sparkContext.textFile(tmpFolder + "page")
                    .map(new Function<String, ZPageObj>() {

                        @Override
                        public ZPageObj call(String t1) {
                            return ZPageObj.parseFromLogLine(t1);
                        }
                    });

            JavaRDD<ZPageBounceObj> pageBounceRdd = sparkContext.textFile(tmpFolder + "pagebounces")
                    .map(new Function<String, ZPageBounceObj>() {

                        @Override
                        public ZPageBounceObj call(String t1) {
                            return ZPageBounceObj.parseFromLogLine(t1);
                        }
                    });

            JavaRDD<ZPageEntranceObj> pageEntranceRdd = sparkContext.textFile(tmpFolder + "pageentrances")
                    .map(new Function<String, ZPageEntranceObj>() {

                        @Override
                        public ZPageEntranceObj call(String t1) {
                            return ZPageEntranceObj.parseFromLogLine(t1);
                        }
                    });

            JavaRDD<ZPageExitObj> pageExitRdd = sparkContext.textFile(tmpFolder + "pageexits")
                    .map(new Function<String, ZPageExitObj>() {

                        @Override
                        public ZPageExitObj call(String t1) {
                            return ZPageExitObj.parseFromLogLine(t1);
                        }
                    });

            JavaRDD<ZPageTimeObj> pageTimeRdd = sparkContext.textFile(tmpFolder + "pagetotaltimes")
                    .map(new Function<String, ZPageTimeObj>() {

                        @Override
                        public ZPageTimeObj call(String t1) {
                            return ZPageTimeObj.parseFromLogLine(t1);
                        }
                    });


            /*
             * parse tmp file to app table
             */
            JavaRDD<ZAppObj> appRdd = sparkContext.textFile(tmpFolder + "app")
                    .map(new Function<String, ZAppObj>() {
                        @Override
                        public ZAppObj call(String line) {
                            return ZAppObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppUniquePageviewObj> appUniquePageviewRdd = sparkContext.textFile(tmpFolder + "appuniquepageview")
                    .map(new Function<String, ZAppUniquePageviewObj>() {
                        @Override
                        public ZAppUniquePageviewObj call(String line) {
                            return ZAppUniquePageviewObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppTotalVisitorObj> appTotalVisitorRdd = sparkContext.textFile(tmpFolder + "apptotalvisitor")
                    .map(new Function<String, ZAppTotalVisitorObj>() {
                        @Override
                        public ZAppTotalVisitorObj call(String line) {
                            return ZAppTotalVisitorObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppReturnVisitorObj> appReturnVisitorRdd = sparkContext.textFile(tmpFolder + "appreturnvisitor")
                    .map(new Function<String, ZAppReturnVisitorObj>() {
                        @Override
                        public ZAppReturnVisitorObj call(String line) {
                            return ZAppReturnVisitorObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppBounceObj> appBounceRdd = sparkContext.textFile(tmpFolder + "appbounce")
                    .map(new Function<String, ZAppBounceObj>() {
                        @Override
                        public ZAppBounceObj call(String line) {
                            return ZAppBounceObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppEntranceObj> appEntranceRdd = sparkContext.textFile(tmpFolder + "appentrances")
                    .map(new Function<String, ZAppEntranceObj>() {
                        @Override
                        public ZAppEntranceObj call(String line) {
                            return ZAppEntranceObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppExitObj> appExitRdd = sparkContext.textFile(tmpFolder + "appexits")
                    .map(new Function<String, ZAppExitObj>() {
                        @Override
                        public ZAppExitObj call(String line) {
                            return ZAppExitObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppTimeObj> appTimeRdd = sparkContext.textFile(tmpFolder + "apptime")
                    .map(new Function<String, ZAppTimeObj>() {
                        @Override
                        public ZAppTimeObj call(String line) {
                            return ZAppTimeObj.parseFromLogLine(line);
                        }
                    });

            /*
             *parse tmp file to others table
             */
            JavaRDD<ZOSObj> osRdd = sparkContext.textFile(tmpFolder + "os")
                    .map(new Function<String, ZOSObj>() {
                        @Override
                        public ZOSObj call(String line) {
                            return ZOSObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZDeviceObj> deviceRdd = sparkContext.textFile(tmpFolder + "device")
                    .map(new Function<String, ZDeviceObj>() {
                        @Override
                        public ZDeviceObj call(String line) {
                            return ZDeviceObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZBrowserObj> browserRdd = sparkContext.textFile(tmpFolder + "browser")
                    .map(new Function<String, ZBrowserObj>() {
                        @Override
                        public ZBrowserObj call(String line) {
                            return ZBrowserObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZLocationObj> locationRdd = sparkContext.textFile(tmpFolder + "location")
                    .map(new Function<String, ZLocationObj>() {
                        @Override
                        public ZLocationObj call(String line) {
                            return ZLocationObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZLanguageObj> languageRdd = sparkContext.textFile(tmpFolder + "language")
                    .map(new Function<String, ZLanguageObj>() {
                        @Override
                        public ZLanguageObj call(String line) {
                            return ZLanguageObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZReferalObj> referalRdd = sparkContext.textFile(tmpFolder + "referal")
                    .map(new Function<String, ZReferalObj>() {
                        @Override
                        public ZReferalObj call(String line) {
                            return ZReferalObj.parseFromLogLine(line);
                        }
                    });
            
            JavaRDD<ZObject> referalRdd1 = sparkContext.textFile(tmpFolder + "referal")
                    .map(new parseLogFunc(TestObj.class));

            /*
             *join table and write to db
             */
            DataFrame pageDF = sqlContext.createDataFrame(pageRdd, ZPageObj.class);
            DataFrame pageBounceDF = sqlContext.createDataFrame(pageBounceRdd, ZPageBounceObj.class);
            DataFrame pageEntranceDF = sqlContext.createDataFrame(pageEntranceRdd, ZPageEntranceObj.class);
            DataFrame pageExitDF = sqlContext.createDataFrame(pageExitRdd, ZPageExitObj.class);
            DataFrame pageTimeDF = sqlContext.createDataFrame(pageTimeRdd, ZPageTimeObj.class);

            DataFrame appDF = sqlContext.createDataFrame(appRdd, ZAppObj.class);
            DataFrame appUniquePageviewDF = sqlContext.createDataFrame(appUniquePageviewRdd, ZAppUniquePageviewObj.class);
            DataFrame appReturnVisitorDF = sqlContext.createDataFrame(appReturnVisitorRdd, ZAppReturnVisitorObj.class);
            DataFrame appTotalVisitorDF = sqlContext.createDataFrame(appTotalVisitorRdd, ZAppTotalVisitorObj.class);
            DataFrame appBounceDF = sqlContext.createDataFrame(appBounceRdd, ZAppBounceObj.class);
            DataFrame appEntranceDF = sqlContext.createDataFrame(appEntranceRdd, ZAppEntranceObj.class);
            DataFrame appExitDF = sqlContext.createDataFrame(appExitRdd, ZAppExitObj.class);
            DataFrame appTimeDF = sqlContext.createDataFrame(appTimeRdd, ZAppTimeObj.class);

            DataFrame osTable = sqlContext.createDataFrame(osRdd, ZOSObj.class).select("id", "app_id", "date_tracking", "os_type", "sessions");

            DataFrame browserDF = sqlContext.createDataFrame(browserRdd, ZBrowserObj.class).select("id", "app_id", "date_tracking", "browser_type", "sessions");

            DataFrame deviceTable = sqlContext.createDataFrame(deviceRdd, ZDeviceObj.class).select("id", "app_id", "date_tracking", "device_type", "sessions");

            DataFrame locationTable = sqlContext.createDataFrame(locationRdd, ZLocationObj.class).select("id", "app_id", "date_tracking", "location", "sessions");

            DataFrame languageTable = sqlContext.createDataFrame(languageRdd, ZLanguageObj.class).select("id", "app_id", "date_tracking", "language", "sessions");

            DataFrame referalTable = sqlContext.createDataFrame(referalRdd, ZReferalObj.class).select("id", "app_id", "date_tracking", "url_ref", "ref_type", "sessions");

            DataFrame pageTable = pageDF.join(pageBounceDF, pageBounceDF.col("app_id").equalTo(pageDF.col("app_id")).and(pageBounceDF.col("path").equalTo(pageDF.col("path"))), "left_outer")
                    .join(pageEntranceDF, pageEntranceDF.col("app_id").equalTo(pageDF.col("app_id")).and(pageEntranceDF.col("path").equalTo(pageDF.col("path"))), "left_outer")
                    .join(pageExitDF, pageExitDF.col("app_id").equalTo(pageDF.col("app_id")).and(pageExitDF.col("path").equalTo(pageDF.col("path"))), "left_outer")
                    .join(pageTimeDF, pageTimeDF.col("app_id").equalTo(pageDF.col("app_id")).and(pageTimeDF.col("path").equalTo(pageDF.col("path"))), "left_outer")
                    .select(pageDF.col("id"), pageDF.col("app_id"), pageDF.col("path"), pageDF.col("date_tracking"), pageDF.col("pageviews"), pageDF.col("unique_pageviews"), pageBounceDF.col("bounces"), pageEntranceDF.col("entrances"), pageExitDF.col("exits"), pageTimeDF.col("total_time_on_page"))
                    .na().fill(0);

            DataFrame appTable = appDF.join(appUniquePageviewDF, appUniquePageviewDF.col("app_id").equalTo(appDF.col("app_id")), "left_outer")
                    .join(appTotalVisitorDF, appTotalVisitorDF.col("app_id").equalTo(appDF.col("app_id")), "left_outer")
                    .join(appReturnVisitorDF, appReturnVisitorDF.col("app_id").equalTo(appDF.col("app_id")), "left_outer")
                    .join(appBounceDF, appBounceDF.col("app_id").equalTo(appDF.col("app_id")), "left_outer")
                    .join(appEntranceDF, appEntranceDF.col("app_id").equalTo(appDF.col("app_id")), "left_outer")
                    .join(appExitDF, appExitDF.col("app_id").equalTo(appDF.col("app_id")), "left_outer")
                    .join(appTimeDF, appTimeDF.col("app_id").equalTo(appDF.col("app_id")), "left_outer")
                    .select(appDF.col("id"), appDF.col("app_id"), appDF.col("date_tracking"), appDF.col("sessions"), appDF.col("pageviews"), appUniquePageviewDF.col("unique_pageviews"), appTotalVisitorDF.col("total_visitor").minus(appReturnVisitorDF.col("return_visitor")).as("new_visitor"), appReturnVisitorDF.col("return_visitor"), appBounceDF.col("bounces"), appEntranceDF.col("entrances"), appExitDF.col("exits"), appTimeDF.col("total_session_duration"))
                    .na().fill(0);

            String url = "jdbc:mysql://localhost/zanalytics";
            Properties prop = new Properties();
            prop.setProperty("user", "root");
            prop.setProperty("password", "qwe123");

            appTable.write().mode(SaveMode.Append).jdbc(url, "app_overview", prop);
            pageTable.write().mode(SaveMode.Append).jdbc(url, "page_overview", prop);
            osTable.write().mode(SaveMode.Append).jdbc(url, "os", prop);
            browserDF.write().mode(SaveMode.Append).jdbc(url, "browser", prop);
            deviceTable.write().mode(SaveMode.Append).jdbc(url, "device", prop);
            locationTable.write().mode(SaveMode.Append).jdbc(url, "location", prop);
            languageTable.write().mode(SaveMode.Append).jdbc(url, "language", prop);
            referalTable.write().mode(SaveMode.Append).jdbc(url, "referal", prop);
            long time = System.currentTimeMillis() - sparkContext.startTime();
            
            sparkContext.stop();
            sparkContext.close();

            System.err.println(TAG + "join and write to db time: " + new SimpleDateFormat("mm:ss:SSS").format(new Date(time)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ZLogAnalyzer getInstance() {
        return instance;
    }
    
    

    public String appTotalFormat(Row row) {
        StringBuilder result = new StringBuilder();
        result.append(row.getString(0)).append(" ");
        result.append(row.getLong(1));
        return result.toString();
    }

    public String pageFormat(Row row) {
        StringBuilder result = new StringBuilder();
        result.append(row.getString(0)).append(" ");
        result.append(row.getString(1)).append(" ");
        result.append(row.getLong(2));
        return result.toString();
    }

    public String otherFormat(Row row) {
        StringBuilder result = new StringBuilder();
        result.append(row.getString(0)).append(" ");
        result.append(row.getString(1)).append(" ");
        result.append(row.getLong(2));
        return result.toString();
    }
    
    

    public static void main(String... args) {
        try {
            String logPath = "/home/datbt/workspace/ZALogStatistic/ZAlog1";
//            if (args.length < 2)
//                return;
//            
//            
//            String tempFolder = args[0];
//            int mode = Integer.parseInt(args[1]);
//            //System.err.println(tempFolder + "\t" + mode);
//            if (mode == 1)
//                ZLogAnalyzer.getInstance().analyze(logPath, tempFolder);
//            else 
//                ZLogAnalyzer.getInstance().joinTempFileAndWriteToDB(tempFolder);
            
            
            
            String tempFolder = String.valueOf(System.currentTimeMillis()) + "/";
            ZLogAnalyzer.getInstance().analyze(logPath, tempFolder);
            System.gc();
            ZLogAnalyzer.getInstance().joinTempFileAndWriteToDB(tempFolder);
                
        } catch (Exception e) {
            e.printStackTrace();
        }
//        System.out.println("test:");
//        for (String s : args){
//            System.err.println(s);
//        }
        
//        long now = System.currentTimeMillis();
//        String logPath = "/home/datbt/workspace/ZALogStatistic/ZAlog2";
//        String tempFolder = String.valueOf(System.currentTimeMillis()) + "/";
//        
//        ZLogAnalyzer statistic = ZLogAnalyzer.getInstance();
//        statistic.analyze(logPath, tempFolder);
////        while (true) {            
////            if (statistic.analyzeDone())
////                break;
////        }
////        
////        
////        System.gc();
////        statistic.joinTempFileAndWriteToDB(tempFolder);
//        System.err.println("time = " + (System.currentTimeMillis() - now));
    }
    
    class parseLogFunc implements Function<String, ZObject>{
        private Class<ZObject> zObjClass;
        
        public parseLogFunc(Class zObjClass){
            this.zObjClass = zObjClass;
        }
        
        @Override
        public ZObject call(String t1){
            try {
                zObjClass.newInstance().parseFromLogLine(t1);
            } catch (InstantiationException | IllegalAccessException ex) {
                ex.printStackTrace();
            }
            return null;
        }
        
    }

}
