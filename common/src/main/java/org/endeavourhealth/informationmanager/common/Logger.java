package org.endeavourhealth.informationmanager.common;

public class Logger {
  public static void info(String info) {
    System.out.println(info);
  }

  public static void error(String error) {
    System.err.println(error);
  }
}
