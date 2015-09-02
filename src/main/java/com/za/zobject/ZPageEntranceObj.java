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
public class ZPageEntranceObj extends ZObject implements Serializable{
    private String app_id;
    private String path;
    private int entrances;

    private static String TAG = "[tag]";
    
    public ZPageEntranceObj(String app_id, String path, String bounces) {
        this.app_id = app_id;
        this.path = path;
        this.entrances = Integer.parseInt(bounces);
    }

    public ZPageEntranceObj() {
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public int getEntrances() {
        return entrances;
    }

    public void setEntrances(int entrances) {
        this.entrances = entrances;
    }
    
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    
    private static final String LOG_ENTRY_PATTERN
            = "(\\S+) (\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    @Override
    public ZPageEntranceObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ZPageEntranceObj(m.group(1), m.group(2), m.group(3));
    }
}
