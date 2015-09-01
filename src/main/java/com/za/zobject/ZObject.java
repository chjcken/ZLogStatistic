/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.za.zobject;

/**
 *
 * @author datbt
 */
public abstract class ZObject {
    public abstract ZObject parseFromLogLine(String logline);
    
    
//    public static String ac (Class<ZObject> zclass){
//        try {
//            return zclass.newInstance().parseFromLogLine("");
//        } catch (InstantiationException | IllegalAccessException ex) {
//            Logger.getLogger(ZObject.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return "error";
//    }
//    
//    
//    public static void main (String... args){
//        System.err.println(ZObject.ac(ZObject.class));
//    }
}

