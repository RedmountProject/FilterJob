/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package filterjob;

/**
 *
 * @author amaury
 */
public class Pair<K, V> {
  private K key;
  private V data;

  public Pair() {
  }

  public Pair(K key, V data) {
    this.key = key;
    this.data = data;
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public V getData() {
    return data;
  }

  public void setData(V data) {
    this.data = data;
  }
}
