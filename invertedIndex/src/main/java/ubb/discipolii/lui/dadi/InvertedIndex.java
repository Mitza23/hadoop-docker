package ubb.discipolii.lui.dadi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: InvertedIndex <input_path> <temp_output_path> <final_output_path> <stopwords_path>");
            System.exit(2);
        }

        Path inputPath = new Path(args[0]);
        Path tempOutputPath = new Path(args[1]);
        Path finalOutputPath = new Path(args[2]);
        Path stopWordsPath = new Path(args[3]);

        Configuration conf = getConf();

        // Job 1
        Job job1 = Job.getInstance(conf, "Line Number Indexer");
        job1.setJarByClass(InvertedIndex.class);
        job1.setMapperClass(Job1Mapper.class);
        job1.setReducerClass(Job1Reducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, tempOutputPath);

        DistributedCache.addCacheFile(stopWordsPath.toUri(), job1.getConfiguration());

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2
        Job job2 = Job.getInstance(conf, "Inverted Index");
        job2.setJarByClass(InvertedIndex.class);
        job2.setMapperClass(Job2Mapper.class);
        job2.setReducerClass(Job2Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, tempOutputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);

        return job2.waitForCompletion(true) ? 0 : 1;
    }
}

