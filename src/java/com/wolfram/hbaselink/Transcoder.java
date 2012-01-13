package com.wolfram.hbaselink;

import java.io.IOException;


public interface Transcoder {
  public Object decode(byte[] data) throws IOException;
  public byte[] encode(Object... objects) throws IOException;
}
