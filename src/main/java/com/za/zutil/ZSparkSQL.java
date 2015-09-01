/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.zutil;

import static com.za.zutil.ZName.*;

/**
 *
 * @author datbt
 */
public class ZSparkSQL {

    public static final String PAGE_SQL = "select " + APP_ID + ","+ PATH +",COUNT(*) as " + PAGEVIEWS 
            + ",COUNT(DISTINCT " + SESSION_ID + ") as " + UNIQUE_PAGEVIEWS + " from " 
            + LOG_TABLE +" group by " + APP_ID +"," + PATH;

    public static final String PAGE_BOUNCE_TEMP_SQL = "select " + SESSION_ID + " from " + LOG_TABLE 
            + "group by " + SESSION_ID + " having COUNT(*)=1";

    public static final String PAGE_ENTRANCE_AND_EXIT_TEMP_SQL = "select " + SESSION_ID 
            + ",min(" + HIT_TIME + ") as " + MIN_TIME + ", max(" + HIT_TIME + ") as " 
            + MAX_TIME + " from " + LOG_TABLE + " group by " + SESSION_ID;

    public static final String PAGE_TOTAL_TIME_SQL = "select " + APP_ID + "," + LAST_PATH 
            + ",SUM(" + LAST_PATH_DURATION + ") as " + TOTAL_TIME_ON_PAGE + " from " + LOG_TABLE 
            + " group by " + APP_ID + "," + LAST_PATH;

    public static final String PAGE_BOUNCE_SQL = "select tk." + APP_ID + ",tk." + PATH 
            + ",COUNT(tk." + SESSION_ID + ") as " + BOUNCES + " from " + PAGE_BOUNCE_TEMP 
            + " as tbl, " + LOG_TABLE + " tk where tk." + SESSION_ID + " = tbl." + SESSION_ID 
            + " group by tk." + APP_ID + ",tk." + PATH;

    public static final String PAGE_ENTRANCE_SQL = "select tk." + APP_ID + ",tk." + PATH 
            + ",count(tk." + SESSION_ID + ") as " + ENTRANCES + " from " + PAGE_ENTRANCE_AND_EXIT_TEMP 
            + " as temp, " + LOG_TABLE + " tk where temp." + SESSION_ID + "=tk." + SESSION_ID 
            + " and temp." + MIN_TIME + "=tk." + HIT_TIME + " group by tk." + APP_ID + ",tk." + PATH;

    public static final String PAGE_EXIT_SQL = "select tk." + APP_ID + ",tk." + PATH 
            + ",count(tk." + SESSION_ID + ") as " + ENTRANCES + " from " + PAGE_ENTRANCE_AND_EXIT_TEMP 
            + " as temp, " + LOG_TABLE + " tk where temp." + SESSION_ID + "=tk." + SESSION_ID 
            + " and temp." + MAX_TIME + "=tk." + HIT_TIME + " group by tk." + APP_ID + ",tk." + PATH;

    public static final String APP_RETURN_VISITOR_SQL = "select " + APP_ID + ",count(" + VISITOR_ID 
            + ") as " + RETURN_VISITOR + " from " + APP_RETURN_VISITOR_TEMP 
            + " as tlbTmp group by " + APP_ID;

    public static final String OS_SQL = "select " + APP_ID + "," + OS_TYPE + ",count(distinct " 
            + SESSION_ID + ") as " + SESSIONS + " from " + LOG_TABLE + " group by " 
            + APP_ID + "," + OS_TYPE;
    
    public static final String BROWSER_SQL = "select " + APP_ID + "," + BROWSER_TYPE + ",count(distinct " 
            + SESSION_ID + ") as " + SESSIONS + " from " + LOG_TABLE + " group by " 
            + APP_ID + "," + BROWSER_TYPE;
    
    public static final String LOCATION_SQL = "select " + APP_ID + "," + LOCATION + ",count(distinct " 
            + SESSION_ID   + ") as " + SESSIONS + " from " + LOG_TABLE + " group by " 
            + APP_ID + "," + LOCATION;
    
    public static final String DEVICE_SQL = "select " + APP_ID + "," + DEVICE_TYPE + ",count(distinct " 
            + SESSION_ID + ") as " + SESSIONS + " from " + LOG_TABLE + " group by " 
            + APP_ID + "," + DEVICE_TYPE;
    
    public static final String LANGUAGE_SQL = "select " + APP_ID + "," + LANG + ",count(distinct " 
            + SESSION_ID + ") as " + SESSIONS + " from " + LOG_TABLE + " group by " 
            + APP_ID + "," + LANG;

    public static final String REFERAL_SQL = "select " + APP_ID + "," + URL_REF + "," + REF_TYPE 
            + ",count(distinct " + SESSION_ID 
            + ") as " + SESSIONS + " from " + LOG_TABLE + " group by " + APP_ID + "," 
            + URL_REF + "," + REF_TYPE;

    public static final String APP_SQL = "select " + APP_ID + ",count(distinct " + SESSION_ID 
            + ") as " + SESSIONS + ", COUNT(*) as " + PAGEVIEWS + " from " + LOG_TABLE 
            + "  group by " + APP_ID;

    public static final String APP_TOTAL_VISITOR_SQL = "select " + APP_ID + ",count(distinct " + VISITOR_ID 
            + ") as " + TOTAL_VISITOR + " from " + LOG_TABLE + " group by " + APP_ID;

    public static final String APP_RETURN_VISITOR_TEMP_SQL = "select " + APP_ID + "," + VISITOR_ID
            + "from " + LOG_TABLE + " group by " + APP_ID + "," + VISITOR_ID 
            + " having min(" + NEW_VISITOR + ")=0";

}
