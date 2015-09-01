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
public class ZAppObj implements Serializable{
    private String app_id;
    private int sessions;
    private int pageviews;
    private int id = 0;
    private String date_tracking = new Timestamp(System.currentTimeMillis()).toString();

    private static String TAG = "[tag]";
    
    public ZAppObj(String app_id, String sessions, String pageviews) {
        this.app_id = app_id;
        this.sessions = Integer.parseInt(sessions);
        this.pageviews = Integer.parseInt(pageviews);
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
    }

    public int getPageviews() {
        return pageviews;
    }

    public void setPageviews(int pageviews) {
        this.pageviews = pageviews;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDate_tracking() {
        return date_tracking;
    }

    public void setDate_tracking(String date_tracking) {
        this.date_tracking = date_tracking;
    }

   
    

    
    private static final String LOG_ENTRY_PATTERN
            = "(\\S+) (\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static ZAppObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }
        return new ZAppObj(m.group(1), m.group(2), m.group(3));
    }
}
