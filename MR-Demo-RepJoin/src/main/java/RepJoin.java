import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RepJoin extends Configured implements Tool {

  private static final Logger logger = LogManager.getLogger(RepJoin.class);
  // change the filter here
  private static final int MAX = 50000;
  // set aws file path
  private static final String FILE_CACHE_PATH = "s3://elricbucket/input/edges.csv";

  public static class cacheMapper extends Mapper<Object, Text, Object, Text> {

    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] edges = value.toString().split(",");
      if (Integer.parseInt(edges[0]) <= MAX && Integer.parseInt(edges[1]) <= MAX) {
        // don't care key, use
        context.write(NullWritable.get(), value);
      }
    }
  }

  /**
   * Use enum class to set the global counter. https://diveintodata.org/2011/03/15/an-example-of-hadoop-mapreduce-counter/
   */
  public enum COUNTER {
    TRIANGLE
  }

  public static class TriangleMapper extends Mapper<Object, Text, Text, Text> {

    // use map to store all the edges
    private final Map<String, List<String>> map = new HashMap<>();

    // https://buhrmann.github.io/hadoop-distributed-cache.html
    @Override
    protected void setup(Context context) throws IllegalArgumentException {
      try {
        // get the cache files locations
        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles == null || cacheFiles.length == 0) {
          // maybe not appropriate to use IAE
          throw new IllegalArgumentException("File not exists.");
        }

        for (URI cf : cacheFiles) {
          FileSystem fs = FileSystem.get(cf, context.getConfiguration());
          Path path = new Path(cf.toString());
          BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
          String line = br.readLine();
          while (line != null) {
            String[] edges = line.split(",");
            if (Integer.parseInt(edges[0]) <= MAX && Integer.parseInt(edges[1]) <= MAX) {
              if (!map.containsKey(edges[0])) {
                // if not exists, create the list
                List<String> list = new LinkedList<>();
                list.add(edges[1]);
                map.put(edges[0], list);
              } else {
                map.get(edges[0]).add(edges[1]);
              }
            }
            // read next line
            line = br.readLine();
          }
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("File not exists.");
      }
    }


    @Override
    protected void map(Object key, Text value, Context context) {
      String[] edges = value.toString().split(",");
      // for each (id1, id2), we get the list of id2, for each id3 in that list,
      // we get the list of id3 and see if id1 in that list of id3.
      if (Integer.parseInt(edges[0]) <= MAX && Integer.parseInt(edges[1]) <= MAX) {
        if (map.containsKey(edges[1])) {
          for (String id3 : map.get(edges[1])) {
            if (map.containsKey(id3)) {
              List<String> id3List = map.get(id3);
              // this eliminates the possible (xid, yid, xid) triangle
              // if id1 is xid, id2 is yid, id3 is xid again, xid's list can not contain itself
              // since there is no self following thing in twitter
              if (id3List.contains(edges[0])) {
                context.getCounter(COUNTER.TRIANGLE).increment(1);
              }
            }
          }
        }
      }
    }
  }


  // only necessary when running locally
  private int cacheFileJob(String input, String output)
      throws IOException, InterruptedException, ClassNotFoundException {
    final Configuration conf = getConf();
    final Job cacheFileJob = Job.getInstance(conf, "cacheFileJob");
    cacheFileJob.setJarByClass(RepJoin.class);
    cacheFileJob.setMapperClass(cacheMapper.class);
    cacheFileJob.setOutputKeyClass(NullWritable.class);
    cacheFileJob.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(cacheFileJob, new Path(input + "/edges.csv"));
    FileOutputFormat.setOutputPath(cacheFileJob, new Path(output + "/Temp"));
    return cacheFileJob.waitForCompletion(true) ? 0 : 1;
  }

  private int TriangleJob(String input, String output)
      throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
    final Configuration conf = getConf();
    final Job triangleJob = Job.getInstance(conf, "TriangleJob");
    triangleJob.setJarByClass(RepJoin.class);
    triangleJob.setMapperClass(TriangleMapper.class);

    triangleJob.setMapOutputKeyClass(Text.class);
    triangleJob.setMapOutputValueClass(Text.class);

    // configure file path
    FileInputFormat.addInputPath(triangleJob, new Path(input + "/edges.csv"));
    FileOutputFormat.setOutputPath(triangleJob, new Path(output + "/Final"));

    // retrieve file cache
    FileSystem fs = FileSystem.get(new URI(FILE_CACHE_PATH), conf);
    RemoteIterator<LocatedFileStatus> remoteIterator =
        fs.listFiles(new Path(FILE_CACHE_PATH), true);
    while (remoteIterator.hasNext()) {
      LocatedFileStatus locatedFileStatus = remoteIterator.next();
      triangleJob.addCacheFile(locatedFileStatus.getPath().toUri());
    }

    triangleJob.waitForCompletion(true);
    return 1;
  }


  @Override
  public int run(final String[] args) throws Exception {
    this.TriangleJob(args[0], args[1]);
    return 0;
  }


  public static void main(final String[] args) {
    if (args.length != 2) {
      throw new Error("Two arguments required:\n<input-dir> <output-dir>");
    }

    try {
      ToolRunner.run(new RepJoin(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}