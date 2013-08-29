/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package filterjob;

import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author amaury
 */
public class FilterDriver extends Configured implements Tool {

    public static ArrayList<Book> BookList;
    public static final Logger LOG = Logger.getLogger(FilterDriver.class.getName());

    static enum Counters {

        CONTENT_EMPTY,
        BOOK_MATCH_COUNT,};

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        //Parse the default Filter configuration file "filter-default.xml"
        Configuration FilterConf = FilterConfiguration.create();

        int res = ToolRunner.run(FilterConf, new FilterDriver(), args);
        System.exit(res);

    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = getConf();

        conf.set("mapred.map.tasks", "5");

        DistributedCache.addCacheFile(new URI(conf.get("filter.path")), conf);
        URI[] i = DistributedCache.getCacheFiles(conf);
        if (i != null) {
            LOG.log(Level.INFO, i.length + " File Added To DistributedCache");
        } else {
            LOG.log(Level.INFO, "No Cache File");
        }

        String db_addr = conf.get("insert.db.address");
        String db_database = conf.get("insert.db.name");
        String table_name = conf.get("insert.db.table.name", "book_content");
        String username = conf.get("insert.db.username");
        String password = conf.get("insert.db.password");

        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://" + db_addr + "/" + db_database + "?user=" + username + "&password=" + password);

        Job job = new Job(conf, conf.get("instance.agent.name", "Filter"));

        String[] fields = {"book_id", "content"};

        DBOutputFormat.setOutput(job, table_name, fields);

        job.setJarByClass(FilterDriver.class);

        //Mapper Outputs
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Book.class);

        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        SequenceFileInputFormat.setInputPaths(job, conf.get("input.path"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        return 0;

    }
}
