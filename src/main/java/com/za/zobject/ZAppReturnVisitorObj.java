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
public class ZAppReturnVisitorObj implements Serializable{
    private String app_id;
    private int return_visitor;

    private static String TAG = "[tag]";
    
    public ZAppReturnVisitorObj(String app_id, String visitor) {
        this.app_id = app_id;
        this.return_visitor = Integer.parseInt(visitor);
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public int getReturn_visitor() {
        return return_visitor;
    }

    public void setReturn_visitor(int return_visitor) {
        this.return_visitor = return_visitor;
    }

  
    
    private static final String LOG_ENTRY_PATTERN
            = "(\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static ZAppReturnVisitorObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ZAppReturnVisitorObj(m.group(1), m.group(2));
    }
}
