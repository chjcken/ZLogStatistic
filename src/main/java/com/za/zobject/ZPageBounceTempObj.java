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
public class ZPageBounceTempObj implements Serializable{
    private String idvisit;

    private static String TAG = "[tag]";
    
    public ZPageBounceTempObj(String idvisit) {
        this.idvisit = idvisit;
    }

    public String getIdvisit() {
        return idvisit;
    }

    public void setIdvisit(String idvisit) {
        this.idvisit = idvisit;
    }



    

    
    private static final String LOG_ENTRY_PATTERN
            = "(\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static ZPageBounceTempObj parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ZPageBounceTempObj(m.group(1));
    }
}
