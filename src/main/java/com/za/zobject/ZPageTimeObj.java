/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.zobject;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author datbt
 */
public class ZPageTimeObj implements Serializable{
    private String app_id;
    private String path;
    private long total_time_on_page;

    private static String TAG = "[tag]";
    
    public ZPageTimeObj(String app_id, String path, String time) {
        this.app_id = app_id;
        this.path = path;
        this.total_time_on_page = Long.parseLong(time);
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

    public long getTotal_time_on_page() {
        return total_time_on_page;
    }

    public void setTotal_time_on_page(long total_time_on_page) {
        this.total_time_on_page = total_time_on_page;
    }

    
    private static final String LOG_ENTRY_PATTERN
            = "(\\S+) (\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static ZPageTimeObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ZPageTimeObj(m.group(1), m.group(2), m.group(3));
    }
}
