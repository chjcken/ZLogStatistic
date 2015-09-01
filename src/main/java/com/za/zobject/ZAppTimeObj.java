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
public class ZAppTimeObj implements Serializable{
    private String app_id;
    private long total_session_duration;

    private static String TAG = "[tag]";
    
    public ZAppTimeObj(String app_id, String time) {
        this.app_id = app_id;
        this.total_session_duration = Long.parseLong(time);
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public long getTotal_session_duration() {
        return total_session_duration;
    }

    public void setTotal_session_duration(long total_session_duration) {
        this.total_session_duration = total_session_duration;
    }
    
    private static final String LOG_ENTRY_PATTERN
            = "(\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static ZAppTimeObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ZAppTimeObj(m.group(1), m.group(2));
    }
}
