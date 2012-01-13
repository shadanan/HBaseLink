package com.wolfram.hbaselink;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.typedbytes.TypedBytesInput;
import org.apache.hadoop.typedbytes.TypedBytesOutput;

public class TypedBytes implements Transcoder {
  @Override
  public Object decode(byte[] data) throws IOException {
    TypedBytesInput tbin = new TypedBytesInput(new DataInputStream(new ByteArrayInputStream(data)));
    ArrayList<Object> result = new ArrayList<Object>();
    
    Object item = null;
    while ((item = tbin.read()) != null) {
      result.add(item);
    }
    
    return result.toArray();
  }

  @Override
  public byte[] encode(Object... objects) throws IOException {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    TypedBytesOutput tbout = new TypedBytesOutput(new DataOutputStream(result));

    for (Object obj : objects) {
      tbout.write(obj);
    }
    
    return result.toByteArray();
  }
}
