import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RSJoin extends Configured implements Tool {

  private static final Logger logger = LogManager.getLogger(RSJoin.class);
  // change the filter here
  private static final int MAX = 50000;

  /**
   * This is the mapper class for create path2 (start, mid, end).
   */
  private static class Path2Mapper extends Mapper<Object, Text, Text, Text> {

    private final Text first = new Text();
    private final Text second = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] edges = value.toString().split(",");
      // filter by id
      if (Integer.parseInt(edges[0]) <= MAX && Integer.parseInt(edges[1]) <= MAX) {
        // for example, given (xid, yid), we emit twice.
        // (xid, (xid, yid, "first")) to match the first id in the tuple, thus xid is key
        // (yid, (xid, yid, "second")) to match the second id in the tuple, thus yid is key
        first.set(edges[0]);
        context.write(first, new Text(edges[0] + "," + edges[1] + "," + "first"));

        second.set(edges[1]);
        context.write(second, new Text(edges[0] + "," + edges[1] + "," + "second"));

      }
    }
  }

  /**
   * This is the reducer class for create path2 (start, mid, end).
   */
  public static class Path2Reducer extends Reducer<Text, Text, Object, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      List<Text> firstList = new LinkedList<>();
      List<Text> secondList = new LinkedList<>();

      for (Text val : values) {
        String[] edges = val.toString().split(",");
        if (edges[2].equals("first")) {
          firstList.add(new Text(edges[0] + "," + edges[1]));
        } else {
          secondList.add(new Text(edges[0] + "," + edges[1]));
        }
      }

      for (Text sec : secondList) {
        for (Text fir : firstList) {
          /**
           * NullWritable is a special type of Writable, as it has a zero-length serialization. No 
           * bytes are written to, or read from, the stream. It is used as a placeholder; for 
           * example, in MapReduce, a key or a value can be declared as a NullWritable when you 
           * don’t need to use that position—it effectively stores a constant empty value. 
           * NullWritable can also be useful as a key in SequenceFile when you want to store a list 
           * of values, as opposed to key-value pairs. It is an immutable singleton: the instance 
           * can be retrieved by calling NullWritable.get()
           */
          // filter the possible scenario which (xid, yid) in first list and (yid, xid) in second
          // list, this is not a valid path2
          String[] firEdges = fir.toString().split(",");
          String[] secEdges = sec.toString().split(",");
          if (!firEdges[1].equals(secEdges[0])) {
            context.write(NullWritable.get(), new Text(secEdges[0] + "," + fir.toString()));
          }
        }
      }

    }
  }

  /**
   * Mapper for the path2 file.
   */
  public static class Path2TriangleMapper extends Mapper<Object, Text, Text, Text> {

    private final Text path2key = new Text();
    private final Text path2val = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] path2 = value.toString().split(",");
      // reverse this one so that we don't need to split edges
      // match the format of edges
      path2key.set(new Text(path2[2] + "," + path2[0]));
      path2val.set(new Text(value.toString() + "," + "path2"));
      context.write(path2key, path2val);
    }
  }

  /**
   * Mapper for the edges.
   */
  public static class EdgeTriangleMapper extends Mapper<Object, Text, Text, Text> {

    private final Text result = new Text();

    @Override
    public void map(final Object key, final Text value, final Context context)
        throws IOException, InterruptedException {

      final String[] edges = value.toString().split(",");
      // filer again
      if (Integer.parseInt(edges[0]) <= MAX && Integer.parseInt(edges[1]) <= MAX) {
        result.set(new Text(value.toString() + "," + "edge"));
        context.write(value, result);
      }
    }
  }


  /**
   * Use enum class to set the global counter. https://diveintodata.org/2011/03/15/an-example-of-hadoop-mapreduce-counter/
   */
  public enum COUNTER {
    TRIANGLE
  }

  /**
   * The reducer to count the total triangles.
   */
  public static class TriangleReducer extends Reducer<Text, Text, Object, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // we only care about numbers, so just calculate the number of outputs
      int path2Count = 0;
      int edgesCount = 0;

      for (Text val : values) {
        String[] records = val.toString().split(",");
        if (records.length == 3) {
          edgesCount++;
        } else {
          path2Count++;
        }
      }

      context.getCounter(COUNTER.TRIANGLE).increment(path2Count * edgesCount);

    }
  }

  private int Path2Job(String input, String output) throws Exception {
    final Configuration conf = getConf();
    final Job path2Job = Job.getInstance(conf, "Path2Job");
    path2Job.setJarByClass(RSJoin.class);
    // since we have three mappers, set the right one
    MultipleInputs.addInputPath(path2Job, new Path(input + "/edges.csv"),
        TextInputFormat.class, Path2Mapper.class);
    path2Job.setReducerClass(Path2Reducer.class);
    path2Job.setMapOutputKeyClass(Text.class);
    path2Job.setMapOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(path2Job, new Path(output + "/Temp"));
    // returns only when job finished
    return path2Job.waitForCompletion(true) ? 0 : 1;
  }

  private int TriangleJob(String input, String output) throws Exception {
    final Configuration conf = getConf();
    final Job triangleJob = Job.getInstance(conf, "TriangleJob");
    triangleJob.setJarByClass(RSJoin.class);

    MultipleInputs.addInputPath(triangleJob, new Path(input + "/edges.csv"),
        TextInputFormat.class, EdgeTriangleMapper.class);
    // use the previous output as input
    MultipleInputs.addInputPath(triangleJob, new Path(output + "/Temp"),
        TextInputFormat.class, Path2TriangleMapper.class);
    triangleJob.setReducerClass(TriangleReducer.class);
    triangleJob.setMapOutputKeyClass(Text.class);
    triangleJob.setMapOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(triangleJob, new Path(output + "/Final"));
    triangleJob.waitForCompletion(true);

    // get the counter
    Counters cn = triangleJob.getCounters();
    Counter sum = cn.findCounter(COUNTER.TRIANGLE);

    System.out.println(sum.getDisplayName() + ":" + sum.getValue());

    return 1;

  }


  @Override
  public int run(final String[] args) throws Exception {
    if (this.Path2Job(args[0], args[1]) == 0) {
      this.TriangleJob(args[0], args[1]);
    }
    return 0;
  }


  public static void main(final String[] args) {
    if (args.length != 2) {
      throw new Error("Two arguments required:\n<input-dir> <output-dir>");
    }

    try {
      ToolRunner.run(new RSJoin(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }

}