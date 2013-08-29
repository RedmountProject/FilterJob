/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package filterjob;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author amaury
 */
public class FilterConfiguration {

    public static final String UUID_KEY = "instance.name";

    private FilterConfiguration() {
    }                 // singleton

    /*
     * Configuration.hashCode() doesn't return values that correspond to a
     * unique set of parameters. This is a workaround so that we can track
     * instances of Configuration created by Filter Job.
     */
    private static void setUUID(Configuration conf) {
        UUID uuid = UUID.randomUUID();
        conf.set(UUID_KEY, uuid.toString());
    }

    /**
     * Retrieve a Filter Job UUID of this configuration object,
     * or null if the configuration was created elsewhere.
     *
     * @param conf configuration instance
     * @return uuid or null
     */
    public static String getUUID(Configuration conf) {
        return conf.get(UUID_KEY);
    }

    /**
     * Create a {@link Configuration} for Filter Job. This will
     * load the standard Filter resources,
     * <code>filter-default.xml</code>
     */
    public static Configuration create() {
        Configuration conf = new Configuration();
        setUUID(conf);
        addFilterResources(conf);
        return conf;
    }

    /**
     * Create a {@link Configuration} from supplied properties.
     *
     * @param addFilterResources if true, then
     * <code>filter-default.xml</code>, will be loaded prior to applying the
     * properties. Otherwise these resources won't be used.
     * @param FilterProperties a set of properties to define (or override)
     */
    public static Configuration create(boolean addFilterResources, Properties FilterProperties) {
        Configuration conf = new Configuration();
        setUUID(conf);
        if (addFilterResources) {
            addFilterResources(conf);
        }
        for (Map.Entry<Object, Object> e : FilterProperties.entrySet()) {
            conf.set(e.getKey().toString(), e.getValue().toString());
        }
        return conf;
    }

    /**
     * Add the standard Filter Job resources to {@link Configuration}.
     *
     * @param conf Configuration object to which configuration is to be added.
     */
    private static Configuration addFilterResources(Configuration conf) {
        conf.addResource(new Path("conf/filter-default.xml"));
        return conf;
    }
}
