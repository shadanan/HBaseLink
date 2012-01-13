package com.wolfram.hbaselink;

public interface Transcoder {
  public Object decode(byte[] data);
  public byte[] encode(Object... objects);
}
