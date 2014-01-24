package de.tuberlin.dima.ml.pact.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import eu.stratosphere.types.IntValue;

import org.apache.commons.codec.binary.Base64;

import eu.stratosphere.types.Value;


public final class PactUtils {
  
  public static final IntValue pactZero = new IntValue(0);
  public static final IntValue pactOne = new IntValue(1);
  
  public static String encodeValueAsBase64(Value object) {

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream outStream = new DataOutputStream(byteStream);
    try {
      object.write(outStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Base64.encodeBase64String(byteStream.toByteArray());
  }
  
  @SuppressWarnings("unchecked")
public static <T extends Value> T decodeValueFromBase64(String encoded, Class<T> type) {
    
    byte[] bytes = Base64.decodeBase64(encoded);
    ByteArrayInputStream byteInStream = new ByteArrayInputStream(bytes);
    DataInputStream inStream = new DataInputStream(byteInStream);

    Value object = null;
    try {
      object = type.newInstance();
    } catch (InstantiationException e1) {
      e1.printStackTrace();
    } catch (IllegalAccessException e1) {
      e1.printStackTrace();
    }
    try {
      object.read(inStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return (T)object;
  }

}
