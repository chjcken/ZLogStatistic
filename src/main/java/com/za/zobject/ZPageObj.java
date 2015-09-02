/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.zobject;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author datbt
 */
public class ZPageObj extends ZObject implements Serializable{
    private String app_id;
    private String path;
    private String date_tracking = new Timestamp(System.currentTimeMillis()).toString();
    private int pageviews;
    private int unique_pageviews;

    private static String TAG = "[tag]";
    
    public ZPageObj(String app_id, String path, String pageviews, String unique_pageviews) {
        this.app_id = app_id;
        this.path = path;
        this.pageviews = Integer.parseInt(pageviews);
        this.unique_pageviews = Integer.parseInt(unique_pageviews);
    }

    public ZPageObj() {
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getPageviews() {
        return pageviews;
    }

    public void setPageviews(int pageviews) {
        this.pageviews = pageviews;
    }

    public int getUnique_pageviews() {
        return unique_pageviews;
    }

    public void setUnique_pageviews(int unique_pageviews) {
        this.unique_pageviews = unique_pageviews;
    }

    public String getDate_tracking() {
        return date_tracking;
    }

    public void setDate_tracking(String date_tracking) {
        this.date_tracking = date_tracking;
    }

    

    
    private static final String LOG_ENTRY_PATTERN
            = "(\\S+) (\\S+) (\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    @Override
    public ZPageObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ZPageObj(m.group(1), m.group(2), m.group(3), m.group(4));
    }
}
