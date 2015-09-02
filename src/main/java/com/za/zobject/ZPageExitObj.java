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
public class ZPageExitObj extends ZObject implements Serializable{
    private String app_id;
    private String path;
    private int exits;

    private static String TAG = "[tag]";
    
    public ZPageExitObj(String app_id, String path, String bounces) {
        this.app_id = app_id;
        this.path = path;
        this.exits = Integer.parseInt(bounces);
    }

    public ZPageExitObj() {
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

    public int getExits() {
        return exits;
    }

    public void setExits(int exits) {
        this.exits = exits;
    }

  

    
    private static final String LOG_ENTRY_PATTERN
            = "(\\S+) (\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    @Override
    public ZPageExitObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ZPageExitObj(m.group(1), m.group(2), m.group(3));
    }
}
