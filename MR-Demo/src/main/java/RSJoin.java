import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RSJoin extends Configured implements Tool {

  private static final Logger logger = LogManager.getLogger(RSJoin.class);
  // change the filter here
  private static final int MAX = 60000;

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
  
  public static class 


  @Override
  public int run(final String[] args) throws Exception {
    final Configuration conf = getConf();
    final Job job = Job.getInstance(conf, "Follower Count");
    job.setJarByClass(RSJoin.class);
    final Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
    // Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
    // ================
    job.setMapperClass(TokenizerMapper.class);
    // set a local combiner
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  //TODO: Add a second map reduce job for soring the result by value

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