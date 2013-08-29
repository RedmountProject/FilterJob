/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package filterjob;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author amaury
 */
public class FilterMapper extends Mapper<LongWritable, webpage, IntWritable, Book> {

    public static final Logger LOG = Logger.getLogger(FilterMapper.class.getName());
    private ArrayList<Book> BookList;
    private Path[] distributedCacheFiles;
    private String title, url, content;

    @Override
    protected void map(LongWritable key, webpage value, Context context) throws IOException, InterruptedException {
        title = value.get_title();
        url = value.get_id();
        content = value.get_text();

        for (Book ABook : BookList) {
            if (ABook.Match(content, url, title)) {
                context.getCounter(FilterDriver.Counters.BOOK_MATCH_COUNT).increment(1);
                LOG.log(Level.INFO, "A Book Matched id : " + ABook.getId() + "  Content : " + content+ "   key : "+key.toString());
                context.write(ABook.getId(), ABook);
                break;
            }
        }
    }

    @Override
    protected void cleanup(Context context) {
        BookList = null;
        distributedCacheFiles = null;
    }

    @Override
    protected void setup(
            Context context)
            throws IOException, InterruptedException {

        //Useful If we have multiple Filter File ;
        //Setup define wich object is biggest and then cache the smallest
        distributedCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());


        if (distributedCacheFiles != null) {
            LOG.log(Level.INFO, distributedCacheFiles.length + " Local File Found");
        } else {
            LOG.log(Level.INFO, "No Cached File Found");
            return;
        }
        BookList = new ArrayList<>();
        for (Path distFile : distributedCacheFiles) {
            File distributedCacheFile = new File(distFile.toString());
            DistributedCacheFileReader reader =
                    getDistributedCacheReader();
            reader.init(distributedCacheFile);
                            
            for (Pair p : (Iterable<Pair>) reader) {
                BookList.add((Book) p.getData());
            }
            reader.close();
        }
    }

    public DistributedCacheFileReader getDistributedCacheReader() {
        return new FilterDistributedCacheFileReader();
    }
}
