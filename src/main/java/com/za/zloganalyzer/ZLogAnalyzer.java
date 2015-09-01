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
import static com.za.zutil.ZName.*;
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

    private static final ZLogAnalyzer instance = new ZLogAnalyzer();


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

    public void parseLogAndWriteDataToTemp(String logSource, String tempFolder) {
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

            JavaRDD<ZLogObject> rawLogRdd = sparkContext.textFile(logSource)
                    .map(new Function<String, ZLogObject>() {
                        @Override
                        public ZLogObject call(String line) throws Exception {
                            return ZLogObject.parseFromLogLine(line);
                        }
                    });

            DataFrame logDataFrame = 
                    sqlContext.createDataFrame(rawLogRdd, ZLogObject.class)
                    .persist(StorageLevel.MEMORY_AND_DISK_SER_2());
            logDataFrame.registerTempTable(LOG_TABLE);
            


            DataFrame page = sqlContext.sql(PAGE_SQL);
            
            DataFrame pageBouncesTemp = sqlContext.sql(PAGE_BOUNCE_TEMP_SQL);
            
            DataFrame pageEntranceAndExitTemp = sqlContext.sql(PAGE_ENTRANCE_AND_EXIT_TEMP_SQL)
                    .persist(StorageLevel.MEMORY_AND_DISK_SER_2());
            
            
            
            DataFrame pageTotalTime = sqlContext.sql(PAGE_TOTAL_TIME_SQL);

            DataFrame app = sqlContext.sql(APP_SQL);
            DataFrame appUniquePageviews = page.groupBy(APP_ID).sum(UNIQUE_PAGEVIEWS);
            DataFrame appTotalVisitor = sqlContext.sql(APP_TOTAL_VISITOR_SQL);
            
            DataFrame appReturnVisitorTemp = sqlContext.sql(APP_RETURN_VISITOR_TEMP_SQL);
            
            DataFrame appTime = pageTotalTime.groupBy(APP_ID).sum(TOTAL_TIME_ON_PAGE);

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
            }).saveAsTextFile(tempFolder + PAGE);
            
            
            pageBouncesTemp.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row row) {
                    StringBuilder result = new StringBuilder();
                    result.append(row.getString(0));
                    return result.toString();
                }
            }).saveAsTextFile(tempFolder + PAGE_BOUNCE_TEMP);
            
            pageEntranceAndExitTemp.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row row) {
                    StringBuilder result = new StringBuilder();
                    result.append(row.getString(0)).append(" ");
                    result.append(row.getLong(1)).append(" ");
                    result.append(row.getLong(2));
                    return result.toString();
                }
            }).saveAsTextFile(tempFolder + PAGE_ENTRANCE_AND_EXIT_TEMP);

            pageEntranceAndExitTemp.unpersist();
            
            pageTotalTime.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return pageFormat(t1);
                }
            }).saveAsTextFile(tempFolder + PAGE_TOTAL_TIME);

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
            }).saveAsTextFile(tempFolder + APP);
            
            
            
            appReturnVisitorTemp.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row row) {
                    StringBuilder result = new StringBuilder();
                    result.append(row.getString(0)).append(" ");
                    result.append(row.getString(1));
                    return result.toString();
                }
            }).saveAsTextFile(tempFolder + APP_RETURN_VISITOR_TEMP);


            appTotalVisitor.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + APP_TOTAL_VISITOR);

            appUniquePageviews.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + APP_UNIQUE_PAGEVIEW);


            appTime.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + APP_TOTAL_TIME);

            /*
             * save other to temp
             */
            os.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) throws Exception {
                    return otherFormat(t1);
                }
            }).saveAsTextFile(tempFolder + OS);

            device.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) throws Exception {
                    return otherFormat(t1);
                }
            }).saveAsTextFile(tempFolder + DEVICE);

            browser.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) throws Exception {
                    return otherFormat(t1);
                }
            }).saveAsTextFile(tempFolder + BROWSER);

            location.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) throws Exception {
                    return otherFormat(t1);
                }
            }).saveAsTextFile(tempFolder + LOCATION);

            language.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) throws Exception {
                    return otherFormat(t1);
                }
            }).saveAsTextFile(tempFolder + LANGUAGE);

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
            }).saveAsTextFile(tempFolder + REFERAL);
            
            
            
            
            readTempFileAndContinueCalculateData(sparkContext, sqlContext, tempFolder);

            
            logDataFrame.unpersist();
            

            long time = System.currentTimeMillis() - sparkContext.startTime();

            
            //sparkContext.close();
            sparkContext.stop();            
            
            System.err.println(TAG + "analyze raw log time: " + new SimpleDateFormat("hh:mm:ss").format(new Date(time)));

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
    
    
    public void readTempFileAndContinueCalculateData(JavaSparkContext sparkContext, SQLContext sqlContext, String tempFolder){
        try {
            JavaRDD<ZPageBounceTempObj> pageBounceTempRdd = sparkContext.textFile(tempFolder+PAGE_BOUNCE_TEMP)
                    .map(new Function<String, ZPageBounceTempObj>() {

                        @Override
                        public ZPageBounceTempObj call(String t1) {
                            return ZPageBounceTempObj.parseFromLogLine(t1);
                        }
                    });
            JavaRDD<ZPageEntranceAndExitTempObj> pageEntranceAndExitTempRdd = sparkContext.textFile(tempFolder+PAGE_ENTRANCE_AND_EXIT_TEMP)
                    .map(new Function<String, ZPageEntranceAndExitTempObj>() {

                        @Override
                        public ZPageEntranceAndExitTempObj call(String t1) {
                            return ZPageEntranceAndExitTempObj.parseFromLogLine(t1);
                        }
                    });
            
            JavaRDD<ZAppReturnVisitorTempObj> appReturnVisitorTempRdd = sparkContext.textFile(tempFolder+APP_RETURN_VISITOR_TEMP)
                    .map(new Function<String, ZAppReturnVisitorTempObj>() {

                        @Override
                        public ZAppReturnVisitorTempObj call(String t1) {
                            return ZAppReturnVisitorTempObj.parseFromLogLine(t1);
                        }
                    });
            sqlContext.createDataFrame(pageBounceTempRdd, ZPageBounceTempObj.class).registerTempTable(PAGE_BOUNCE_TEMP);
            sqlContext.createDataFrame(pageEntranceAndExitTempRdd, ZPageEntranceAndExitTempObj.class).registerTempTable(PAGE_ENTRANCE_AND_EXIT_TEMP);
            sqlContext.createDataFrame(appReturnVisitorTempRdd, ZAppReturnVisitorTempObj.class).registerTempTable(APP_RETURN_VISITOR_TEMP);
            
           
            DataFrame pageBounds = sqlContext.sql(PAGE_BOUNCE_SQL);
            DataFrame pageEntrances = sqlContext.sql(PAGE_ENTRANCE_SQL);
            DataFrame pageExits = sqlContext.sql(PAGE_EXIT_SQL);
            
            DataFrame appReturnVisitor = sqlContext.sql(APP_RETURN_VISITOR_SQL);
            DataFrame appBounds = pageBounds.groupBy(APP_ID).sum(BOUNCES);
            DataFrame appEntrances = pageEntrances.groupBy(APP_ID).sum(ENTRANCES);
            DataFrame appExits = pageExits.groupBy(APP_ID).sum(EXITS);
            
            
            //save page
            pageBounds.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return pageFormat(t1);
                }
            }).saveAsTextFile(tempFolder + PAGE_BOUNCE);

            pageEntrances.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return pageFormat(t1);
                }
            }).saveAsTextFile(tempFolder + PAGE_ENTRANCE);

            pageExits.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return pageFormat(t1);
                }
            }).saveAsTextFile(tempFolder + PAGE_EXIT);
            
            //save app
            appReturnVisitor.toJavaRDD().map(new Function<Row, String>() {
//
                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + APP_RETURN_VISITOR);
            
            
            appBounds.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + APP_BOUNCE);

        
            
            appEntrances.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + APP_ENTRANCE);

            appExits.toJavaRDD().map(new Function<Row, String>() {

                @Override
                public String call(Row t1) {
                    return appTotalFormat(t1);
                }
            }).saveAsTextFile(tempFolder + APP_EXIT);            
            
            
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
            JavaRDD<ZPageObj> pageRdd = sparkContext.textFile(tmpFolder + PAGE)
                    .map(new Function<String, ZPageObj>() {

                        @Override
                        public ZPageObj call(String t1) {
                            return ZPageObj.parseFromLogLine(t1);
                        }
                    });

            JavaRDD<ZPageBounceObj> pageBounceRdd = sparkContext.textFile(tmpFolder + PAGE_BOUNCE)
                    .map(new Function<String, ZPageBounceObj>() {

                        @Override
                        public ZPageBounceObj call(String t1) {
                            return ZPageBounceObj.parseFromLogLine(t1);
                        }
                    });

            JavaRDD<ZPageEntranceObj> pageEntranceRdd = sparkContext.textFile(tmpFolder + PAGE_ENTRANCE)
                    .map(new Function<String, ZPageEntranceObj>() {

                        @Override
                        public ZPageEntranceObj call(String t1) {
                            return ZPageEntranceObj.parseFromLogLine(t1);
                        }
                    });

            JavaRDD<ZPageExitObj> pageExitRdd = sparkContext.textFile(tmpFolder + PAGE_EXIT)
                    .map(new Function<String, ZPageExitObj>() {

                        @Override
                        public ZPageExitObj call(String t1) {
                            return ZPageExitObj.parseFromLogLine(t1);
                        }
                    });

            JavaRDD<ZPageTimeObj> pageTimeRdd = sparkContext.textFile(tmpFolder + PAGE_TOTAL_TIME)
                    .map(new Function<String, ZPageTimeObj>() {

                        @Override
                        public ZPageTimeObj call(String t1) {
                            return ZPageTimeObj.parseFromLogLine(t1);
                        }
                    });


            /*
             * parse tmp file to app table
             */
            JavaRDD<ZAppObj> appRdd = sparkContext.textFile(tmpFolder + APP)
                    .map(new Function<String, ZAppObj>() {
                        @Override
                        public ZAppObj call(String line) {
                            return ZAppObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppUniquePageviewObj> appUniquePageviewRdd = sparkContext.textFile(tmpFolder + APP_UNIQUE_PAGEVIEW)
                    .map(new Function<String, ZAppUniquePageviewObj>() {
                        @Override
                        public ZAppUniquePageviewObj call(String line) {
                            return ZAppUniquePageviewObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppTotalVisitorObj> appTotalVisitorRdd = sparkContext.textFile(tmpFolder + APP_TOTAL_VISITOR)
                    .map(new Function<String, ZAppTotalVisitorObj>() {
                        @Override
                        public ZAppTotalVisitorObj call(String line) {
                            return ZAppTotalVisitorObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppReturnVisitorObj> appReturnVisitorRdd = sparkContext.textFile(tmpFolder + APP_RETURN_VISITOR)
                    .map(new Function<String, ZAppReturnVisitorObj>() {
                        @Override
                        public ZAppReturnVisitorObj call(String line) {
                            return ZAppReturnVisitorObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppBounceObj> appBounceRdd = sparkContext.textFile(tmpFolder + APP_BOUNCE)
                    .map(new Function<String, ZAppBounceObj>() {
                        @Override
                        public ZAppBounceObj call(String line) {
                            return ZAppBounceObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppEntranceObj> appEntranceRdd = sparkContext.textFile(tmpFolder + APP_ENTRANCE)
                    .map(new Function<String, ZAppEntranceObj>() {
                        @Override
                        public ZAppEntranceObj call(String line) {
                            return ZAppEntranceObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppExitObj> appExitRdd = sparkContext.textFile(tmpFolder + APP_EXIT)
                    .map(new Function<String, ZAppExitObj>() {
                        @Override
                        public ZAppExitObj call(String line) {
                            return ZAppExitObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZAppTimeObj> appTimeRdd = sparkContext.textFile(tmpFolder + APP_TOTAL_TIME)
                    .map(new Function<String, ZAppTimeObj>() {
                        @Override
                        public ZAppTimeObj call(String line) {
                            return ZAppTimeObj.parseFromLogLine(line);
                        }
                    });

            /*
             *parse tmp file to others table
             */
            JavaRDD<ZOSObj> osRdd = sparkContext.textFile(tmpFolder + OS)
                    .map(new Function<String, ZOSObj>() {
                        @Override
                        public ZOSObj call(String line) {
                            return ZOSObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZDeviceObj> deviceRdd = sparkContext.textFile(tmpFolder + DEVICE)
                    .map(new Function<String, ZDeviceObj>() {
                        @Override
                        public ZDeviceObj call(String line) {
                            return ZDeviceObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZBrowserObj> browserRdd = sparkContext.textFile(tmpFolder + BROWSER)
                    .map(new Function<String, ZBrowserObj>() {
                        @Override
                        public ZBrowserObj call(String line) {
                            return ZBrowserObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZLocationObj> locationRdd = sparkContext.textFile(tmpFolder + LOCATION)
                    .map(new Function<String, ZLocationObj>() {
                        @Override
                        public ZLocationObj call(String line) {
                            return ZLocationObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZLanguageObj> languageRdd = sparkContext.textFile(tmpFolder + LANGUAGE)
                    .map(new Function<String, ZLanguageObj>() {
                        @Override
                        public ZLanguageObj call(String line) {
                            return ZLanguageObj.parseFromLogLine(line);
                        }
                    });

            JavaRDD<ZReferalObj> referalRdd = sparkContext.textFile(tmpFolder + REFERAL)
                    .map(new Function<String, ZReferalObj>() {
                        @Override
                        public ZReferalObj call(String line) {
                            return ZReferalObj.parseFromLogLine(line);
                        }
                    });

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

            DataFrame osTable = sqlContext.createDataFrame(osRdd, ZOSObj.class).select("id", APP_ID, DATE, OS_TYPE, SESSIONS);

            DataFrame browserDF = sqlContext.createDataFrame(browserRdd, ZBrowserObj.class).select("id", APP_ID, DATE, BROWSER_TYPE, SESSIONS);

            DataFrame deviceTable = sqlContext.createDataFrame(deviceRdd, ZDeviceObj.class).select("id", APP_ID, DATE, DEVICE_TYPE, SESSIONS);

            DataFrame locationTable = sqlContext.createDataFrame(locationRdd, ZLocationObj.class).select("id", APP_ID, DATE, LOCATION, SESSIONS);

            DataFrame languageTable = sqlContext.createDataFrame(languageRdd, ZLanguageObj.class).select("id", APP_ID, DATE, LANGUAGE, SESSIONS);

            DataFrame referalTable = sqlContext.createDataFrame(referalRdd, ZReferalObj.class).select("id", APP_ID, DATE, URL_REF, REF_TYPE, SESSIONS);

            DataFrame pageTable = pageDF.join(pageBounceDF, pageBounceDF.col(APP_ID).equalTo(pageDF.col(APP_ID)).and(pageBounceDF.col(PATH).equalTo(pageDF.col(PATH))), "left_outer")
                    .join(pageEntranceDF, pageEntranceDF.col(APP_ID).equalTo(pageDF.col(APP_ID)).and(pageEntranceDF.col(PATH).equalTo(pageDF.col(PATH))), "left_outer")
                    .join(pageExitDF, pageExitDF.col(APP_ID).equalTo(pageDF.col(APP_ID)).and(pageExitDF.col(PATH).equalTo(pageDF.col(PATH))), "left_outer")
                    .join(pageTimeDF, pageTimeDF.col(APP_ID).equalTo(pageDF.col(APP_ID)).and(pageTimeDF.col(PATH).equalTo(pageDF.col(PATH))), "left_outer")
                    .select(pageDF.col("id"), pageDF.col(APP_ID), pageDF.col(PATH), pageDF.col(DATE), pageDF.col(PAGEVIEWS), pageDF.col(UNIQUE_PAGEVIEWS), pageBounceDF.col(BOUNCES), pageEntranceDF.col(ENTRANCES), pageExitDF.col(EXITS), pageTimeDF.col(TOTAL_TIME_ON_PAGE))
                    .na().fill(0);

            DataFrame appTable = appDF.join(appUniquePageviewDF, appUniquePageviewDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appTotalVisitorDF, appTotalVisitorDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appReturnVisitorDF, appReturnVisitorDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appBounceDF, appBounceDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appEntranceDF, appEntranceDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appExitDF, appExitDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appTimeDF, appTimeDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .select(appDF.col("id"), appDF.col(APP_ID), appDF.col(DATE), appDF.col(SESSIONS), appDF.col(PAGEVIEWS), appUniquePageviewDF.col(UNIQUE_PAGEVIEWS), appTotalVisitorDF.col(TOTAL_VISITOR).minus(appReturnVisitorDF.col(RETURN_VISITOR)).as(NEW_VISITOR), appReturnVisitorDF.col(RETURN_VISITOR), appBounceDF.col(BOUNCES), appEntranceDF.col(ENTRANCES), appExitDF.col(EXITS), appTimeDF.col(TOTAL_SESSION_DURATION))
                    .na().fill(0);

            String url = "jdbc:mysql://localhost/zanalytics";
            Properties prop = new Properties();
            prop.setProperty("user", "root");
            prop.setProperty("password", "qwe123");

            appTable.write().mode(SaveMode.Append).jdbc(url, APP_OVERVIEW_TABLE, prop);
            pageTable.write().mode(SaveMode.Append).jdbc(url, PAGE_OVERVIEW_TABLE, prop);
            osTable.write().mode(SaveMode.Append).jdbc(url, OS, prop);
            browserDF.write().mode(SaveMode.Append).jdbc(url, BROWSER, prop);
            deviceTable.write().mode(SaveMode.Append).jdbc(url, DEVICE, prop);
            locationTable.write().mode(SaveMode.Append).jdbc(url, LOCATION, prop);
            languageTable.write().mode(SaveMode.Append).jdbc(url, LANGUAGE, prop);
            referalTable.write().mode(SaveMode.Append).jdbc(url, REFERAL, prop);
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
            String logPath = "/home/datbt/workspace/ZALogStatistic/ZAlog";
//            if (args.length < 2)
//                return;
//            
//            
//            String tempFolder = args[0];
//            int mode = Integer.parseInt(args[1]);
//            //System.err.println(tempFolder + "\t" + mode);
//            if (mode == 1)
//                ZLogAnalyzer.getInstance().parseLogAndWriteDataToTemp(logPath, tempFolder);
//            else 
//                ZLogAnalyzer.getInstance().joinTempFileAndWriteToDB(tempFolder);
            
            
            
            String tempFolder = String.valueOf(System.currentTimeMillis()) + "/";
            ZLogAnalyzer.getInstance().parseLogAndWriteDataToTemp(logPath, tempFolder);
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
//        statistic.parseLogAndWriteDataToTemp(logPath, tempFolder);
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
    
//    class parseLogFunc implements Function<String, ZObject>{
//        private Class<ZObject> zObjClass;
//        
//        public parseLogFunc(Class zObjClass){
//            this.zObjClass = zObjClass;
//        }
//        
//        @Override
//        public ZObject call(String t1){
//            try {
//                zObjClass.newInstance().parseFromLogLine(t1);
//            } catch (InstantiationException | IllegalAccessException ex) {
//                ex.printStackTrace();
//            }
//            return null;
//        }
//        
//    }

}
