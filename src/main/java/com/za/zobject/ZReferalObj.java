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
public class ZReferalObj implements Serializable{
    private String app_id;
    private String url_ref;
    private int ref_type;
    private int sessions;
    private int id = 0;
    private String date_tracking = new Timestamp(System.currentTimeMillis()).toString();

    private static String TAG = "[tag]";
    
    public ZReferalObj(String app_id, String url_ref, String type, String sessions) {
        this.app_id = app_id;
        this.sessions = Integer.parseInt(sessions);
        this.url_ref = url_ref;
        this.ref_type = Integer.parseInt(type);
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

    public String getOs_type() {
        return url_ref;
    }

    public void setOs_type(String os_type) {
        this.url_ref = os_type;
    }

    public String getUrl_ref() {
        return url_ref;
    }

    public void setUrl_ref(String url_ref) {
        this.url_ref = url_ref;
    }

    public int getRef_type() {
        return ref_type;
    }

    public void setRef_type(int ref_type) {
        this.ref_type = ref_type;
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
            = "(\\S+) (\\S+) (\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static ZReferalObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }
        return new ZReferalObj(m.group(1), m.group(2), m.group(3), m.group(4));
    }
}
