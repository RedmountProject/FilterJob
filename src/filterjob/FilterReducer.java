/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package filterjob;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 *
 * @author amaury
 */
public class FilterReducer extends Reducer<IntWritable, Book, Book, IntWritable> {

    public static final Logger LOG = Logger.getLogger(FilterReducer.class.getName());

    @Override
    public void reduce(IntWritable key, Iterable<Book> values, Context context) throws IOException, InterruptedException {

        Iterator<Book> it = values.iterator();
        while (it.hasNext()) {
            Book b = (Book) it.next();
            LOG.log(Level.INFO, "BookId = " + key + " Url = " + b.geturl());
            context.write(b, key);
        }
    }
}
