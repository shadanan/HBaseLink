package com.wolfram.hbaselink;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

public class HBaseLink {
  private Configuration conf = null;
  private HashMap<String, Object> cache = null;
  
  public HBaseLink(String[]... params) {
    System.setProperty(
        "javax.xml.parsers.DocumentBuilderFactory", 
        "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
    System.setProperty(
        "javax.xml.transform.TransformerFactory", 
        "com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl");
    
    conf = new Configuration();
    for (int i = 0; i < params.length; i++) {
      conf.set(params[i][0], params[i][1]);
    }
    cache = new HashMap<String, Object>();
  }
  
  public Configuration getConf() {
    return conf;
  }
  
  public HashMap<String, Object> getCache() {
    return cache;
  }
}
