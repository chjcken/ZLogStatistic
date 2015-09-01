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
import com.za.zobject.ZAppTimeObj;
import com.za.zobject.ZLanguageObj;
import com.za.zobject.ZAppExitObj;
import com.za.zobject.ZPageExitObj;
import com.za.zobject.ZAppObj;
import com.za.zobject.ZLocationObj;
import com.za.zobject.ZAppReturnVisitorObj;
import com.za.zobject.ZAppEntranceObj;
import com.za.zobject.ZPageBounceObj;
import com.za.zobject.ZBrowserObj;
import com.za.zobject.ZReferalObj;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 *
 * @author datbt
 */
public class Import {

    public void joinTempFileAndWriteToDB(String tmpFolder) {

        try {
            SparkConf sparkConf;
            JavaSparkContext sparkContext;
            SQLContext sqlContext;

            sparkConf = new SparkConf()
                    .setMaster("local[2]")
                    .setAppName(ZLogAnalyzer.class.getName()).set("spark.executor.memory", "1g");
            sparkConf.set("spark.storage.memoryFraction", "0.4");
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
                    .select(pageDF.col("id"), pageDF.col("app_id"), pageDF.col("path"), pageDF.col("date_tracking"), pageBounceDF.col("bounces"), pageEntranceDF.col("entrances"), pageExitDF.col("exits"), pageTimeDF.col("total_time_on_page"))
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
            osTable.show();

            sparkContext.stop();
            sparkContext.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String... args) {
        try {
            String tempFolder = String.valueOf(System.currentTimeMillis()) + "/";
            ProcessBuilder pb = new ProcessBuilder("java", "-Xmx3g", "-cp", "ZALogStatistic-1.0-SNAPSHOT.jar", "com/za/zloganalyzer/ZLogAnalyzer", tempFolder, "1");
            pb.directory(new File("/home/datbt/workspace/ZALogStatistic/target"));
            pb.redirectOutput(new File("phase1"));
//          pb.redirectError(new File("out"));
            Process process = pb.start();
            int phase1Status = process.waitFor();
            
            if (phase1Status != 0)
                return;
                
            pb = new ProcessBuilder("java", "-Xmx3g", "-cp", "ZALogStatistic-1.0-SNAPSHOT.jar", "com/za/zloganalyzer/ZLogAnalyzer", tempFolder, "2");
            pb.redirectOutput(new File("phase2"));
            
            pb.start();
            
            
            
//            StringBuilder sb = new StringBuilder();
//            BufferedReader br = null;
//            try {
//                br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
//                String line = null;
//                while ((line = br.readLine()) != null) {
//                    //        sb.append(line + System.getProperty("line.separator"));
//                    System.out.println(line);
//                }
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            } finally {
//                br.close();
//            }
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
            
//            Process process = pb.start();

//            BufferedReader reader
//                    = new BufferedReader(new InputStreamReader(process.getErrorStream()));
//            StringBuilder builder = new StringBuilder();
//            String line = null;
//            while ((line = reader.readLine()) != null) {
//                System.out.println(line);
//                //    builder.append(line);
//                //     builder.append(System.getProperty("line.separator"));
//            }
//            // String result = builder.toString();
//
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

  

}
