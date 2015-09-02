/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.zloganalyzer;

import com.za.zobject.ZPageObj;
import com.za.zobject.ZAppTotalVisitorObj;
import com.za.zobject.ZAppUniquePageviewObj;
import com.za.zobject.ZOSObj;
import com.za.zobject.ZPageEntranceObj;
import com.za.zobject.ZAppBounceObj;
import com.za.zobject.ZPageTimeObj;
import com.za.zobject.ZDeviceObj;
import com.za.zobject.ZRawLogObject;
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
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.commons.io.FileUtils;
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

    public static ZLogAnalyzer getInstance() {
        return instance;
    }

    private JavaSparkContext initSparkContext() {
        SparkConf sparkConf;
        sparkConf = new SparkConf()
                .setMaster(MASTER)
                .setAppName(ZLogAnalyzer.class.getName())
                .set("spark.executor.memory", "2g")
                .set("spark.storage.memoryFraction", "0.4");

        JavaSparkContext sparkContext;
        sparkContext = new JavaSparkContext(sparkConf);
        return sparkContext;
    }

    private void closeSpark(JavaSparkContext sparkContext) {
        sparkContext.stop();
        sparkContext.close();
    }

    private void parseLogAndWriteDataToTemp(String logSource, String tempFolder,
            JavaSparkContext sparkContext, SQLContext sqlContext) {
        if (logSource == null) {
            throw new NullPointerException("logSource is null.");
        }

        try {
            JavaRDD<ZObject> rawLogRdd = sparkContext.textFile(logSource)
                    .map(new ParseLogFunction(new ZRawLogObject()));

            DataFrame logDataFrame
                    = sqlContext.createDataFrame(rawLogRdd, ZRawLogObject.class)
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

            pageTotalTime.toJavaRDD().map(new PageAndOtherFormatFunction())
                    .saveAsTextFile(tempFolder + PAGE_TOTAL_TIME);

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

            appTotalVisitor.toJavaRDD().map(new AppFormatFunction())
                    .saveAsTextFile(tempFolder + APP_TOTAL_VISITOR);

            appUniquePageviews.toJavaRDD().map(new AppFormatFunction())
                    .saveAsTextFile(tempFolder + APP_UNIQUE_PAGEVIEW);

            appTime.toJavaRDD().map(new AppFormatFunction())
                    .saveAsTextFile(tempFolder + APP_TOTAL_TIME);

            /*
             * save other to temp
             */
            os.toJavaRDD().map(new PageAndOtherFormatFunction())
                    .saveAsTextFile(tempFolder + OS);

            device.toJavaRDD().map(new PageAndOtherFormatFunction())
                    .saveAsTextFile(tempFolder + DEVICE);

            browser.toJavaRDD().map(new PageAndOtherFormatFunction())
                    .saveAsTextFile(tempFolder + BROWSER);

            location.toJavaRDD().map(new PageAndOtherFormatFunction())
                    .saveAsTextFile(tempFolder + LOCATION);

            language.toJavaRDD().map(new PageAndOtherFormatFunction())
                    .saveAsTextFile(tempFolder + LANGUAGE);

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

            readTempFileAndContinueCalculateData(tempFolder, sparkContext, sqlContext);

            logDataFrame.unpersist();

            long time = System.currentTimeMillis() - sparkContext.startTime();

           
            System.err.println(TAG + "analyze raw log time: " + new SimpleDateFormat("hh:mm:ss").format(new Date(time)));

        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    private void readTempFileAndContinueCalculateData(String tempFolder, JavaSparkContext sparkContext, SQLContext sqlContext) {
        try {
            /*
             * parse temp file
             */
            JavaRDD<ZObject> pageBounceTempRdd = sparkContext.textFile(tempFolder + PAGE_BOUNCE_TEMP)
                    .map(new ParseLogFunction(new ZPageBounceTempObj()));

            JavaRDD<ZObject> pageEntranceAndExitTempRdd = sparkContext.textFile(tempFolder + PAGE_ENTRANCE_AND_EXIT_TEMP)
                    .map(new ParseLogFunction(new ZPageEntranceAndExitTempObj()));

            JavaRDD<ZObject> appReturnVisitorTempRdd = sparkContext.textFile(tempFolder + APP_RETURN_VISITOR_TEMP)
                    .map(new ParseLogFunction(new ZAppReturnVisitorTempObj()));

            sqlContext.createDataFrame(pageBounceTempRdd, ZPageBounceTempObj.class)
                    .registerTempTable(PAGE_BOUNCE_TEMP);
            sqlContext.createDataFrame(pageEntranceAndExitTempRdd, ZPageEntranceAndExitTempObj.class)
                    .registerTempTable(PAGE_ENTRANCE_AND_EXIT_TEMP);
            sqlContext.createDataFrame(appReturnVisitorTempRdd, ZAppReturnVisitorTempObj.class)
                    .registerTempTable(APP_RETURN_VISITOR_TEMP);

            /*
             * calculate data
             */
            DataFrame pageBounds = sqlContext.sql(PAGE_BOUNCE_SQL);
            DataFrame pageEntrances = sqlContext.sql(PAGE_ENTRANCE_SQL);
            DataFrame pageExits = sqlContext.sql(PAGE_EXIT_SQL);

            DataFrame appReturnVisitor = sqlContext.sql(APP_RETURN_VISITOR_SQL);
            DataFrame appBounds = pageBounds.groupBy(APP_ID).sum(BOUNCES);
            DataFrame appEntrances = pageEntrances.groupBy(APP_ID).sum(ENTRANCES);
            DataFrame appExits = pageExits.groupBy(APP_ID).sum(EXITS);

            //save page
            pageBounds.toJavaRDD().map(new PageAndOtherFormatFunction())
                    .saveAsTextFile(tempFolder + PAGE_BOUNCE);

            pageEntrances.toJavaRDD().map(new PageAndOtherFormatFunction())
                    .saveAsTextFile(tempFolder + PAGE_ENTRANCE);

            pageExits.toJavaRDD().map(new PageAndOtherFormatFunction())
                    .saveAsTextFile(tempFolder + PAGE_EXIT);

            //save app
            appReturnVisitor.toJavaRDD().map(new AppFormatFunction())
                    .saveAsTextFile(tempFolder + APP_RETURN_VISITOR);

            appBounds.toJavaRDD().map(new AppFormatFunction())
                    .saveAsTextFile(tempFolder + APP_BOUNCE);

            appEntrances.toJavaRDD().map(new AppFormatFunction())
                    .saveAsTextFile(tempFolder + APP_ENTRANCE);

            appExits.toJavaRDD().map(new AppFormatFunction())
                    .saveAsTextFile(tempFolder + APP_EXIT);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void joinResultTempFileAndWriteToDB(String tmpFolder, JavaSparkContext sparkContext,
            SQLContext sqlContext) {
        try {
            long now = System.currentTimeMillis();
//           
            JavaRDD<ZObject> pageRdd = sparkContext.textFile(tmpFolder + PAGE)
                    .map(new ParseLogFunction(new ZPageObj()));

            JavaRDD<ZObject> pageBounceRdd = sparkContext.textFile(tmpFolder + PAGE_BOUNCE)
                    .map(new ParseLogFunction(new ZPageBounceObj()));

            JavaRDD<ZObject> pageEntranceRdd = sparkContext.textFile(tmpFolder + PAGE_ENTRANCE)
                    .map(new ParseLogFunction(new ZPageEntranceObj()));

            JavaRDD<ZObject> pageExitRdd = sparkContext.textFile(tmpFolder + PAGE_EXIT)
                    .map(new ParseLogFunction(new ZPageExitObj()));

            JavaRDD<ZObject> pageTimeRdd = sparkContext.textFile(tmpFolder + PAGE_TOTAL_TIME)
                    .map(new ParseLogFunction(new ZPageTimeObj()));


            /*
             * parse tmp file to app table
             */
            JavaRDD<ZObject> appRdd = sparkContext.textFile(tmpFolder + APP)
                    .map(new ParseLogFunction(new ZAppObj()));

            JavaRDD<ZObject> appUniquePageviewRdd = sparkContext.textFile(tmpFolder + APP_UNIQUE_PAGEVIEW)
                    .map(new ParseLogFunction(new ZAppUniquePageviewObj()));

            JavaRDD<ZObject> appTotalVisitorRdd = sparkContext.textFile(tmpFolder + APP_TOTAL_VISITOR)
                    .map(new ParseLogFunction(new ZAppTotalVisitorObj()));

            JavaRDD<ZObject> appReturnVisitorRdd = sparkContext.textFile(tmpFolder + APP_RETURN_VISITOR)
                    .map(new ParseLogFunction(new ZAppReturnVisitorObj()));

            JavaRDD<ZObject> appBounceRdd = sparkContext.textFile(tmpFolder + APP_BOUNCE)
                    .map(new ParseLogFunction(new ZAppBounceObj()));

            JavaRDD<ZObject> appEntranceRdd = sparkContext.textFile(tmpFolder + APP_ENTRANCE)
                    .map(new ParseLogFunction(new ZAppEntranceObj()));

            JavaRDD<ZObject> appExitRdd = sparkContext.textFile(tmpFolder + APP_EXIT)
                    .map(new ParseLogFunction(new ZAppExitObj()));

            JavaRDD<ZObject> appTimeRdd = sparkContext.textFile(tmpFolder + APP_TOTAL_TIME)
                    .map(new ParseLogFunction(new ZAppTimeObj()));

            /*
             *parse tmp file to others table
             */
            JavaRDD<ZObject> osRdd = sparkContext.textFile(tmpFolder + OS)
                    .map(new ParseLogFunction(new ZOSObj()));

            JavaRDD<ZObject> deviceRdd = sparkContext.textFile(tmpFolder + DEVICE)
                    .map(new ParseLogFunction(new ZDeviceObj()));

            JavaRDD<ZObject> browserRdd = sparkContext.textFile(tmpFolder + BROWSER)
                    .map(new ParseLogFunction(new ZBrowserObj()));

            JavaRDD<ZObject> locationRdd = sparkContext.textFile(tmpFolder + LOCATION)
                    .map(new ParseLogFunction(new ZLocationObj()));

            JavaRDD<ZObject> languageRdd = sparkContext.textFile(tmpFolder + LANGUAGE)
                    .map(new ParseLogFunction(new ZLanguageObj()));

            JavaRDD<ZObject> referalRdd = sparkContext.textFile(tmpFolder + REFERAL)
                    .map(new ParseLogFunction(new ZReferalObj()));


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

            DataFrame osTable = sqlContext.createDataFrame(osRdd, ZOSObj.class).select(APP_ID, DATE, OS_TYPE, SESSIONS);

            DataFrame browserDF = sqlContext.createDataFrame(browserRdd, ZBrowserObj.class).select(APP_ID, DATE, BROWSER_TYPE, SESSIONS);

            DataFrame deviceTable = sqlContext.createDataFrame(deviceRdd, ZDeviceObj.class).select(APP_ID, DATE, DEVICE_TYPE, SESSIONS);

            DataFrame locationTable = sqlContext.createDataFrame(locationRdd, ZLocationObj.class).select(APP_ID, DATE, LOCATION, SESSIONS);

            DataFrame languageTable = sqlContext.createDataFrame(languageRdd, ZLanguageObj.class).select(APP_ID, DATE, LANGUAGE, SESSIONS);

            DataFrame referalTable = sqlContext.createDataFrame(referalRdd, ZReferalObj.class).select(APP_ID, DATE, URL_REF, REF_TYPE, SESSIONS);

            DataFrame pageTable = pageDF.join(pageBounceDF, pageBounceDF.col(APP_ID).equalTo(pageDF.col(APP_ID)).and(pageBounceDF.col(PATH).equalTo(pageDF.col(PATH))), "left_outer")
                    .join(pageEntranceDF, pageEntranceDF.col(APP_ID).equalTo(pageDF.col(APP_ID)).and(pageEntranceDF.col(PATH).equalTo(pageDF.col(PATH))), "left_outer")
                    .join(pageExitDF, pageExitDF.col(APP_ID).equalTo(pageDF.col(APP_ID)).and(pageExitDF.col(PATH).equalTo(pageDF.col(PATH))), "left_outer")
                    .join(pageTimeDF, pageTimeDF.col(APP_ID).equalTo(pageDF.col(APP_ID)).and(pageTimeDF.col(PATH).equalTo(pageDF.col(PATH))), "left_outer")
                    .select(pageDF.col(APP_ID), pageDF.col(PATH), pageDF.col(DATE), pageDF.col(PAGEVIEWS), pageDF.col(UNIQUE_PAGEVIEWS), pageBounceDF.col(BOUNCES), pageEntranceDF.col(ENTRANCES), pageExitDF.col(EXITS), pageTimeDF.col(TOTAL_TIME_ON_PAGE))
                    .na().fill(0);

            DataFrame appTable = appDF.join(appUniquePageviewDF, appUniquePageviewDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appTotalVisitorDF, appTotalVisitorDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appReturnVisitorDF, appReturnVisitorDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appBounceDF, appBounceDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appEntranceDF, appEntranceDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appExitDF, appExitDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .join(appTimeDF, appTimeDF.col(APP_ID).equalTo(appDF.col(APP_ID)), "left_outer")
                    .select(appDF.col(APP_ID), appDF.col(DATE), appDF.col(SESSIONS), appDF.col(PAGEVIEWS), appUniquePageviewDF.col(UNIQUE_PAGEVIEWS), appTotalVisitorDF.col(TOTAL_VISITOR).minus(appReturnVisitorDF.col(RETURN_VISITOR)).as(NEW_VISITOR), appReturnVisitorDF.col(RETURN_VISITOR), appBounceDF.col(BOUNCES), appEntranceDF.col(ENTRANCES), appExitDF.col(EXITS), appTimeDF.col(TOTAL_SESSION_DURATION))
                    .na().fill(0);

            /*
             * write final result to db
             */
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
            long time = System.currentTimeMillis() - now;


            System.err.println(TAG + "join and write to db time: " + new SimpleDateFormat("mm:ss:SSS").format(new Date(time)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteTempFile(String tempFolder) throws IOException {
        File temp = new File(tempFolder);
        FileUtils.deleteDirectory(temp);
    }

    private class AppFormatFunction implements Function<Row, String> {

        @Override
        public String call(Row row) {
            StringBuilder result = new StringBuilder();
            result.append(row.getString(0)).append(" ");
            result.append(row.getLong(1));
            return result.toString();
        }
    }

    private class PageAndOtherFormatFunction implements Function<Row, String> {

        @Override
        public String call(Row row) {
            StringBuilder result = new StringBuilder();
            result.append(row.getString(0)).append(" ");
            result.append(row.getString(1)).append(" ");
            result.append(row.getLong(2));
            return result.toString();
        }
    }

    public void analyze(String logPath) {
        String tempFolder = String.valueOf(System.currentTimeMillis()) + "/";
        JavaSparkContext sparkContext;
        SQLContext sqlContext;
        sparkContext = initSparkContext();
        sqlContext = new SQLContext(sparkContext);
        parseLogAndWriteDataToTemp(logPath, tempFolder, sparkContext, sqlContext);

        System.gc();

        joinResultTempFileAndWriteToDB(tempFolder, sparkContext, sqlContext);

        closeSpark(sparkContext);
    }

    public static void main(String... args) {
        try {
            long now = System.currentTimeMillis();
            String logPath = "/home/datbt/workspace/ZALogStatistic/ZAlog1";

            ZLogAnalyzer.getInstance().analyze(logPath);

            System.err.println("analyze time: " + new SimpleDateFormat("hh:mm:ss").format(new Date(System.currentTimeMillis() - now)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private class ParseLogFunction implements Function<String, ZObject> {

        private ZObject zObj;

        public ParseLogFunction(ZObject object) {
            this.zObj = object;
        }

        @Override
        public ZObject call(String t1) {
            return zObj.parseFromLogLine(t1);
        }
    }

}
