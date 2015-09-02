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
public class ZPageEntranceAndExitTempObj extends ZObject implements Serializable{
    private String idvisit;
    private long mintime;
    private long maxtime;

    private static String TAG = "[tag]";
    
    public ZPageEntranceAndExitTempObj(String idvisit, String mintime, String maxtime) {
        this.idvisit = idvisit;
        this.maxtime = Long.parseLong(maxtime);
        this.mintime = Long.parseLong(mintime);
    }

    public ZPageEntranceAndExitTempObj() {
    }

    public String getIdvisit() {
        return idvisit;
    }

    public void setIdvisit(String idvisit) {
        this.idvisit = idvisit;
    }

    public long getMintime() {
        return mintime;
    }

    public void setMintime(long mintime) {
        this.mintime = mintime;
    }

    public long getMaxtime() {
        return maxtime;
    }

    public void setMaxtime(long maxtime) {
        this.maxtime = maxtime;
    }


    

    
    private static final String LOG_ENTRY_PATTERN
            = "(\\S+) (\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    @Override
    public ZPageEntranceAndExitTempObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ZPageEntranceAndExitTempObj(m.group(1), m.group(2), m.group(3));
    }
}
