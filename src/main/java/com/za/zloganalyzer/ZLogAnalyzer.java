/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.zloganalyzer;

import java.io.Serializable;
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

//    private static SparkConf conf;
//    private static JavaSparkContext sparkContext;
//    private static JavaSQLContext sqlContext;
    private static final ZLogAnalyzer instance = new ZLogAnalyzer();

    private String pageOverviewSQL = "SELECT idsite,url,COUNT(*) as pageviews,COUNT(DISTINCT idvisit) as unique_pageviews,SUM(duration) as total_time from logsTable group by idsite,url,path_duration";
    private String osSQL = "SELECT idsite,os,COUNT(DISTINCT idvisit) as sessions from logsTable group by idsite,os";
    private String browserSQL = "SELECT idsite,us_br,COUNT(DISTINCT idvisit) as sessions from logsTable group by idsite,us_br";
    private String locationSQL = "SELECT idsite,ct_city,COUNT(DISTINCT idvisit) as sessions from logsTable group by idsite,ct_city";
    private String deviceSQL = "SELECT idsite,device,COUNT(DISTINCT idvisit) as sessions from logsTable group by idsite,device";
    private String referalSQL = "SELECT idsite,url_ref,COUNT(DISTINCT idvisit) as sessions from logsTable group by idsite,url_ref";
    private String appOverviewSQL = "SELECT idsite,COUNT(DISTINCT idvisit) as sessions, COUNT(*) as pageviews from logsTable  group by idsite";

    private ZLogAnalyzer() {
    }

    public void parse(String logSource) {
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
            
            
            String sql = "SELECT idsite,url,idvisit from logsTable group by idsite,url,idvisit having COUNT(*)=1";
            JavaSchemaRDD temp = sqlContext.sql(sql);
            temp.registerTempTable("temp");
            sqlContext.sqlContext().cacheTable("temp");  
            String sql1 = "SELECT idsite,url,COUNT(*) as bound from temp group by idsite,url";
            List<Row> test = sqlContext.sql(sql1).collect();
            
            
            for (Row row : test) {
                System.err.println(String.format("%s\t%s\t%d\t",
                        row.getString(0), row.getString(1), row.getLong(2)));
            }
            
            
            
            
            //String sql1 = "SELECT idsite,url,COUNT() FROM logs group by iddite,url";
            //calculate pageOverview
//            List<Row> pageOverviewTable = sqlContext.sql(pageOverviewSQL).collect();//missing bound, entrance, exit
//            
//            
//            List<Row> osTable = sqlContext.sql(osSQL).collect();
//            List<Row> referalTable = sqlContext.sql(referalSQL).collect();
//            List<Row> locationTable = sqlContext.sql(locationSQL).collect();
//            List<Row> deviceTable = sqlContext.sql(deviceSQL).collect();
//            List<Row> browserTable = sqlContext.sql(browserSQL).collect();

//            for (Row row : pageOverviewTable) {
//                System.err.println(String.format("%s\t%s\t%d\t%d\t%d",
//                        row.getString(0), row.getString(1), row.getLong(2),
//                        row.getLong(3), row.getLong(4)));
//            }
            
            sparkContext.stop();
        } catch (Exception e) {
            System.err.println(TAG + "error while parsing log: " + e.getMessage());
        }
    }

    public static ZLogAnalyzer getInstance() {
        return instance;
    }

    public static void main(String... args) {
        String path = "ZAlog";
        ZLogAnalyzer.getInstance().parse(path);
    }
}
