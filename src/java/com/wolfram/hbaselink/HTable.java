package com.wolfram.hbaselink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HTable extends org.apache.hadoop.hbase.client.HTable {
  private class Column {
    byte[] family;
    byte[] qualifier;
    
    public Column(byte[] family, byte[] qualifier) {
      this.family = family;
      this.qualifier = qualifier;
    }
    
    @Override
    public int hashCode() {
      return Arrays.hashCode(family) + Arrays.hashCode(qualifier);
    }
    
    @Override
    public boolean equals(Object obj) {
      Column c = (Column)obj;
      return Arrays.equals(family, c.family) && Arrays.equals(qualifier, c.qualifier);
    }
  }
  
  private class Interval implements Comparable<Interval> {
    long start;
    long stop;
    
    public Interval(long start, long stop) {
      if (stop < start) {
        throw new RuntimeException("Invalid interval");
      }
      
      this.start = start;
      this.stop = stop;
    }
    
    @Override
    public int compareTo(Interval i) {
      return new Long(start).compareTo(i.start);
    }
    
    public boolean overlaps(Interval i) {
      return (i.start <= start && i.stop > start ||
          i.start < stop && i.stop >= stop);
    }
  }
  
  private boolean includeKey = true;
  private boolean includeFamily = true;
  private boolean includeQualifier = true;
  private boolean includeTimestamp = true;
  private boolean includeValue = true;
  private int includeSize = 4;
  
  private void updateIncludeSize() {
    includeSize = 0;
    if (includeFamily) includeSize++;
    if (includeQualifier) includeSize++;
    if (includeTimestamp) includeSize++;
    if (includeValue) includeSize++;
  }
  
  public void setIncludeKey(boolean includeKey) {
    this.includeKey = includeKey;
  }
  
  public void setIncludeFamily(boolean includeFamily) {
    this.includeFamily = includeFamily;
    updateIncludeSize();
  }
  
  public void setIncludeQualifier(boolean includeQualifier) {
    this.includeQualifier = includeQualifier;
    updateIncludeSize();
  }
  
  public void setIncludeTimestamp(boolean includeTimestamp) {
    this.includeTimestamp = includeTimestamp;
    updateIncludeSize();
  }
  
  public void setIncludeValue(boolean includeValue) {
    this.includeValue = includeValue;
    updateIncludeSize();
  }
  
  private Transcoder defaultTranscoder = new StringBinary();
  private Transcoder keyTranscoder = null;
  private Transcoder familyTranscoder = null;
  private HashMap<String, Transcoder> qualifierTranscoders = new HashMap<String, Transcoder>();
  private HashMap<Column, Transcoder> fieldTranscoders = new HashMap<Column, Transcoder>();
  private HashMap<Column, TreeMap<Interval, Transcoder>> versionedFieldTranscoders = new HashMap<Column, TreeMap<Interval, Transcoder>>();
  
  public void setKeyTranscoder(Transcoder transcoder) {
    keyTranscoder = transcoder;
  }
  
  public void setFamilyTranscoder(Transcoder transcoder) {
    familyTranscoder = transcoder;
  }
  
  public void setQualifierTranscoder(String family, Transcoder qualifierTranscoder) {
    qualifierTranscoders.put(family, qualifierTranscoder);
  }
  
  public void setTranscoder(byte[] family, byte[] qualifier, Transcoder transcoder) {
    fieldTranscoders.put(new Column(family, qualifier), transcoder);
  }
  
  public void setTranscoder(byte[] family, byte[] qualifier, long start, long stop, Transcoder transcoder) {
    Column column = new Column(family, qualifier);
    Interval interval = new Interval(start, stop);
    
    if (!versionedFieldTranscoders.containsKey(column)) {
      versionedFieldTranscoders.put(column, new TreeMap<Interval, Transcoder>());
    }
    
    TreeMap<Interval, Transcoder> transcoders = versionedFieldTranscoders.get(column);
    Interval prev = transcoders.floorKey(interval);
    
    if (prev != null && prev.overlaps(interval)) {
      throw new RuntimeException("The new interval overlaps an existing interval");
    }
    
    transcoders.put(interval, transcoder);
  }
  
  public Transcoder getKeyTranscoder() {
    if (keyTranscoder == null) return defaultTranscoder;
    return keyTranscoder;
  }
  
  public Transcoder getFamilyTranscoder() {
    if (familyTranscoder == null) return defaultTranscoder;
    return familyTranscoder;
  }
  
  public Transcoder getQualifierTranscoder(String family) {
    if (qualifierTranscoders.containsKey(family)) {
      return qualifierTranscoders.get(family);
    } else {
      return defaultTranscoder;
    }
  }
  
  public Transcoder getTranscoder(byte[] family, byte[] qualifier, long timestamp) {
    Transcoder result = null;
    
    if (result == null)
      result = getTranscoder(new Column(family, qualifier), timestamp);
    
    if (result == null)
      result = getTranscoder(new Column(family, null), timestamp);
    
    if (result == null)
      result = getTranscoder(new Column(family, qualifier));
    
    if (result == null)
      result = getTranscoder(new Column(family, null));
    
    if (result == null)
      result = defaultTranscoder;
    
    return result;
  }
  
  private Transcoder getTranscoder(Column column) {
    return fieldTranscoders.get(column);
  }
  
  private Transcoder getTranscoder(Column column, long timestamp) {
    Interval interval = new Interval(timestamp, timestamp);
    
    TreeMap<Interval, Transcoder> columns = versionedFieldTranscoders.get(column);
    if (columns == null) return null;
    
    Interval key = columns.floorKey(interval);
    if (key.overlaps(interval)) {
      return columns.get(key);
    }
    
    return null;
  }
  
  public HTable(Configuration conf, byte[] tableName) throws IOException {
    super(conf, tableName);
  }
  
  public HTable(byte[] tableName) throws IOException {
    super(tableName);
  }
  
  // Cache variables
  private long count = 0;
  private byte[] row = null;
  private ResultScanner scanner;
  
  private Object[] decodeKeyValues(KeyValue[] kvs, byte[] id) throws IOException {
    Object[] row = null;
    int index = 0;
    
    if (includeKey) {
      row = new Object[kvs.length + 1];
      row[index++] = getKeyTranscoder().decode(id);
    } else {
      row = new Object[kvs.length];
    }
    
    for (KeyValue kv : kvs) {
      byte[] family = kv.getFamily();
      byte[] qualifier = kv.getQualifier();
      long timestamp = kv.getTimestamp();
      byte[] value = kv.getValue();
      
      if (includeSize == 1) {
        if (includeFamily)
          row[index++] = getFamilyTranscoder().decode(family);
        else if (includeQualifier)
          row[index++] = getQualifierTranscoder(Bytes.toStringBinary(family)).decode(qualifier);
        else if (includeTimestamp)
          row[index++] = timestamp;
        else if (includeValue)
          row[index++] = getTranscoder(family, qualifier, timestamp).decode(value);
      } else {
        int findex = 0;
        Object[] column = new Object[includeSize];
        row[index++] = column;
        
        if (includeFamily)
          column[findex++] = getFamilyTranscoder().decode(family);
        if (includeQualifier)
          column[findex++] = getQualifierTranscoder(Bytes.toStringBinary(family)).decode(qualifier);
        if (includeTimestamp)
          column[findex++] = timestamp;
        if (includeValue)
          column[findex++] = getTranscoder(family, qualifier, timestamp).decode(value);
      }
    }
    
    return row;
  }
  
  // Get
  public Object getDecoded(Get get) throws IOException {
    Result result = get(get);
    if (result.isEmpty()) return null;
    return decodeKeyValues(result.raw(), result.getRow());
  }

  // Count
  public long countDecoded(int caching) throws IOException {
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    scan.setCaching(caching);
    scan.setFilter(new FirstKeyOnlyFilter());

    ResultScanner scanner = getScanner(scan);
    
    count = 0;
    Iterator<Result> iterator = scanner.iterator();
    
    while (iterator.hasNext()) {
      Result result = iterator.next();
      row = result.getRow();
      count++;
    }
    
    long result = count;
    count = 0;
    row = null;
    return result;
  }
  
  public Object getCurrentRow() throws IOException {
    if (row == null) {
      return "";
    } else if (keyTranscoder != null) {
      return keyTranscoder.decode(row);
    } else {
      return Bytes.toStringBinary(row);
    }
  }
  
  public long getCurrentCount() {
    return count;
  }
  
  // Scan
  public void setScan(Scan scan) throws IOException {
    count = 0;
    row = null;
    scanner = getScanner(scan);
  }
  
  public Object[] scanDecoded(int size) throws IOException {
    ArrayList<Object> decodedResults;
    if (size == -1) {
      decodedResults = new ArrayList<Object>();
    } else {
      decodedResults = new ArrayList<Object>(size);
    }
    
    for (int i = 0; i < size || size == -1; i++) {
      Result result = scanner.next();
      if (result == null) break;
      
      count++;
      row = result.getRow();
      
      decodedResults.add(decodeKeyValues(result.raw(), row));
    }
    
    return decodedResults.toArray();
  }

  public Object[] scanDecoded() throws IOException {
    return scanDecoded(-1);
  }
}
