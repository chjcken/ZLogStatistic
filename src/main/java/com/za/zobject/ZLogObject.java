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
public class ZLogObject implements Serializable{
    private static final String TAG = "[ZLogObject]\t";
    
    private String id;
    private String app_id;
    private String action_name;
    private String path;
    private int ref_type;
    private String url_ref;
    private int idvc;
    private long viewts;
    private long idts;
    private long idtscr;
    private String idvisit;
    private String res;
    private int java;
    private int fla;
    private int new_visitor;
    private String ct_code;
    private String location;
    private String us_lang;
    private String browser_type;
    private String os_type;
    private String device_type;
    private int duration;
    private String path_duration;

    public ZLogObject(String id, String idsite, String action_name, String url, 
            String ref_type, String urlref, String idvc, String viewts, 
            String idts, String idtscr, String idvisit, String res, 
            String java, String fla, String new_visitor, String ct_code, 
            String ct_city, String us_lang, String us_br, String os, 
            String device, String duration, String path_duration) {
        this.id = id;
        this.app_id = idsite;
        this.action_name = action_name;
        this.path = url;
        this.ref_type = Integer.parseInt(ref_type);
        this.url_ref = urlref;
        this.idvc = Integer.parseInt(idvc);
        this.viewts = Long.parseLong(viewts);
        this.idts = Long.parseLong(idts);
        this.idtscr = Long.parseLong(idtscr);
        this.idvisit = idvisit;
        this.res = res;
        this.java = Integer.parseInt(java);
        this.fla = Integer.parseInt(fla);
        this.new_visitor = Integer.parseInt(new_visitor);
        this.ct_code = ct_code;
        this.location = ct_city;
        this.us_lang = us_lang;
        this.browser_type = us_br;
        this.os_type = os;
        this.device_type = device;
        this.duration = Integer.parseInt(duration);
        this.path_duration = path_duration;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getApp_id() {
        return app_id;
    }

    public void setApp_id(String app_id) {
        this.app_id = app_id;
    }

    public String getAction_name() {
        return action_name;
    }

    public void setAction_name(String action_name) {
        this.action_name = action_name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getRef_type() {
        return ref_type;
    }

    public void setRef_type(int ref_type) {
        this.ref_type = ref_type;
    }

    public String getUrl_ref() {
        return url_ref;
    }

    public void setUrl_ref(String url_ref) {
        this.url_ref = url_ref;
    }

    public int getIdvc() {
        return idvc;
    }

    public void setIdvc(int idvc) {
        this.idvc = idvc;
    }

    public long getViewts() {
        return viewts;
    }

    public void setViewts(long viewts) {
        this.viewts = viewts;
    }

    public long getIdts() {
        return idts;
    }

    public void setIdts(long idts) {
        this.idts = idts;
    }

    public long getIdtscr() {
        return idtscr;
    }

    public void setIdtscr(long idtscr) {
        this.idtscr = idtscr;
    }

    public String getIdvisit() {
        return idvisit;
    }

    public void setIdvisit(String idvisit) {
        this.idvisit = idvisit;
    }

    public String getRes() {
        return res;
    }

    public void setRes(String res) {
        this.res = res;
    }

    public int getJava() {
        return java;
    }

    public void setJava(int java) {
        this.java = java;
    }

    public int getFla() {
        return fla;
    }

    public void setFla(int fla) {
        this.fla = fla;
    }

    public int getNew_visitor() {
        return new_visitor;
    }

    public void setNew_visitor(int new_visitor) {
        this.new_visitor = new_visitor;
    }

    public String getCt_code() {
        return ct_code;
    }

    public void setCt_code(String ct_code) {
        this.ct_code = ct_code;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getUs_lang() {
        return us_lang;
    }

    public void setUs_lang(String us_lang) {
        this.us_lang = us_lang;
    }

    public String getBrowser_type() {
        return browser_type;
    }

    public void setBrowser_type(String browser_type) {
        this.browser_type = browser_type;
    }

    public String getOs_type() {
        return os_type;
    }

    public void setOs_type(String os_type) {
        this.os_type = os_type;
    }

    public String getDevice_type() {
        return device_type;
    }

    public void setDevice_type(String device_type) {
        this.device_type = device_type;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public String getPath_duration() {
        return path_duration;
    }

    public void setPath_duration(String path_duration) {
        this.path_duration = path_duration;
    }



    private static final String LOG_ENTRY_PATTERN
            = "(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static ZLogObject parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            System.err.println(TAG + "error: cannot parse log" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new ZLogObject(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), 
                m.group(6), m.group(7), m.group(8), m.group(9), m.group(10), m.group(11), 
                m.group(12), m.group(13), m.group(14), m.group(15), m.group(16), m.group(17), 
                m.group(18), m.group(19), m.group(20), m.group(21), m.group(22), m.group(23));
    }

    @Override
    public String toString() {
        return String.format("%s %s %s %s %d %s %d %d %d %d %s %s %d %d %d %s %s %s %s %s %s %d %s",
                id, app_id, action_name, path, ref_type, url_ref, idvc, viewts, 
                idts, idtscr, idvisit, res, java, fla, new_visitor, ct_code, 
                location, us_lang, browser_type, os_type, device_type, duration, path_duration);
    }
}
