/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.zloganalyzer;

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
    private String idsite;
    private String action_name;
    private String url;
    private int ref_type;
    private String urlref;
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
    private String ct_city;
    private String us_lang;
    private String us_br;
    private String os;
    private String device;
    private int duration;
    private String path_duration;

    public ZLogObject(String id, String idsite, String action_name, String url, 
            String ref_type, String urlref, String idvc, String viewts, 
            String idts, String idtscr, String idvisit, String res, 
            String java, String fla, String new_visitor, String ct_code, 
            String ct_city, String us_lang, String us_br, String os, 
            String device, String duration, String path_duration) {
        this.id = id;
        this.idsite = idsite;
        this.action_name = action_name;
        this.url = url;
        this.ref_type = Integer.parseInt(ref_type);
        this.urlref = urlref;
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
        this.ct_city = ct_city;
        this.us_lang = us_lang;
        this.us_br = us_br;
        this.os = os;
        this.device = device;
        this.duration = Integer.parseInt(duration);
        this.path_duration = path_duration;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIdsite() {
        return idsite;
    }

    public void setIdsite(String idsite) {
        this.idsite = idsite;
    }

    public String getAction_name() {
        return action_name;
    }

    public void setAction_name(String action_name) {
        this.action_name = action_name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getRef_type() {
        return ref_type;
    }

    public void setRef_type(int ref_type) {
        this.ref_type = ref_type;
    }

    public String getUrlref() {
        return urlref;
    }

    public void setUrlref(String urlref) {
        this.urlref = urlref;
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

    public String getCt_city() {
        return ct_city;
    }

    public void setCt_city(String ct_city) {
        this.ct_city = ct_city;
    }

    public String getUs_lang() {
        return us_lang;
    }

    public void setUs_lang(String us_lang) {
        this.us_lang = us_lang;
    }

    public String getUs_br() {
        return us_br;
    }

    public void setUs_br(String us_br) {
        this.us_br = us_br;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
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
        return String.format(
                "%s %s %s %s %d %s %d %d %d %d %s %s %d %d %d %s %s %s %s %s %s %d %s",
                id, idsite, action_name, url, ref_type, urlref, idvc, viewts, 
                idts, idtscr, idvisit, res, java, fla, new_visitor, ct_code, 
                ct_city, us_lang, us_br, os, device, duration, path_duration);
    }
}
