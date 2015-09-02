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
public class ZAppUniquePageviewObj extends ZObject implements Serializable{
    private String app_id;
    private int unique_pageviews;

    private static String TAG = "[tag]";
    
    public ZAppUniquePageviewObj(String app_id, String unique_pageviews) {
        this.app_id = app_id;
        this.unique_pageviews = Integer.parseInt(unique_pageviews);
    }

    public ZAppUniquePageviewObj() {
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public int getUnique_pageviews() {
        return unique_pageviews;
    }

    public void setUnique_pageviews(int unique_pageviews) {
        this.unique_pageviews = unique_pageviews;
    }

 

    
    private static final String LOG_ENTRY_PATTERN
            = "(\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    @Override
    public ZAppUniquePageviewObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ZAppUniquePageviewObj(m.group(1), m.group(2));
    }
}
