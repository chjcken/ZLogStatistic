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
public class ZLocationObj implements Serializable{
    private String app_id;
    private String location;
    private int sessions;
    private int id = 0;
    private String date_tracking = new Timestamp(System.currentTimeMillis()).toString();

    private static String TAG = "[tag]";
    
    public ZLocationObj(String app_id, String location, String sessions) {
        this.app_id = app_id;
        this.sessions = Integer.parseInt(sessions);
        this.location = location;
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

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
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

    public static ZLocationObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }
        return new ZLocationObj(m.group(1), m.group(2), m.group(3));
    }
}
