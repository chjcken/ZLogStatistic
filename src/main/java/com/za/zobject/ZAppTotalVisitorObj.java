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
public class ZAppTotalVisitorObj extends ZObject implements Serializable{
    private String app_id;
    private int total_visitor;

    private static String TAG = "[tag]";
    
    public ZAppTotalVisitorObj(String app_id, String visitor) {
        this.app_id = app_id;
        this.total_visitor = Integer.parseInt(visitor);
    }

    public ZAppTotalVisitorObj() {
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public int getTotal_visitor() {
        return total_visitor;
    }

    public void setTotal_visitor(int total_visitor) {
        this.total_visitor = total_visitor;
    }

  
    
    private static final String LOG_ENTRY_PATTERN
            = "(\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    @Override
    public ZAppTotalVisitorObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ZAppTotalVisitorObj(m.group(1), m.group(2));
    }
}
