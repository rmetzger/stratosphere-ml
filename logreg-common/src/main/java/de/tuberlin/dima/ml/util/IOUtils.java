package de.tuberlin.dima.ml.util;

import java.io.File;
import java.io.FileFilter;

public class IOUtils {
  
  public static boolean deleteRecursively(String path) {
    return new DeletingVisitor().accept(new File(path));
  }
  
  /**
   * Copied from MahoutTestCase. Recursively deletes folder and contained files
   */
  private static class DeletingVisitor implements FileFilter {
    
    public boolean accept(File f) {
      if (!f.isFile()) {
        f.listFiles(this);
      }
      f.delete();
      return false;
    }
  }
  
  public static String getDirectoryOfJarOrClass(Class<?> clazz) {
    // System.out.println("Classpath: " + System.getProperty("java.class.path"));
    return (new File(clazz.getProtectionDomain().getCodeSource()
        .getLocation().getPath())).getParent();
  }

}
