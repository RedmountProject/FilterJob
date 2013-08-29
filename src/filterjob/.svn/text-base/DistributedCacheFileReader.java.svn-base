/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package filterjob;

/**
 *
 * @author amaury
 */
import java.io.*;

public interface DistributedCacheFileReader<K, V> extends Iterable<Pair<K, V>> {
  public void init(File f) throws IOException;
  public void close();
}