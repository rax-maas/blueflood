package com.cloudkick.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Version {
  public final static int VERSION_MAJOR = 1;
  public final static int VERSION_MINOR = 0;
  public final static int VERSION_PATCH = 0;
  
  private final static String BUILD_PROP_FILE = "com/cloudkick/util/build.properties";
  
  public static String getBuildVersion() throws IOException {
      InputStream in = Version.class.getClassLoader().getResourceAsStream(BUILD_PROP_FILE);
      if (in == null) return null;
      Properties props = new Properties();
      props.load(in);
      return props.getProperty("build");
  }

  public static String getHashVersion() throws IOException {
      String buildVersion = getBuildVersion();
      if (buildVersion != null) {
          String[] version = buildVersion.split("\\.");
          if (version.length > 1) {
              return version[1];
          }
      }
      return null;
  }
  
  public static String getDottedVersion() {
      return String.format("%d.%d.%d", VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH);
  }
  
  public static String getCanonicalVersion() throws IOException {
      String build = getBuildVersion();
      String dottedVersion = getDottedVersion();
      return (build == null) ? dottedVersion : String.format("%s-%s", dottedVersion , build);
  }
}
