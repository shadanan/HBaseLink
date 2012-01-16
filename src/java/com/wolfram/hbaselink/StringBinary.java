package com.wolfram.hbaselink;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

public class StringBinary implements Transcoder {

  @Override
  public Object decode(byte[] data) throws IOException {
    return Bytes.toStringBinary(data);
  }

  @Override
  public byte[] encode(Object... objects) throws IOException {
    if (objects.length != 1 && objects[0].getClass() != String.class) {
      throw new IOException("Transcoder can only encode String objects");
    }
    return Bytes.toBytesBinary((String)objects[0]);
  }
}
