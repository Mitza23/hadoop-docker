package ubb.discipolii.lui.dadi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private Set<String> stopWords = new HashSet<>();
    private Text fileNameKey = new Text();
    private Text offsetLineValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        if (stopWordsFiles != null && stopWordsFiles.length > 0) {
            for (Path stopWordsFile : stopWordsFiles) {
                BufferedReader reader = new BufferedReader(new FileReader(stopWordsFile.toString()));
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
                reader.close();
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
        String line = value.toString().toLowerCase();
        String[] words = line.split("\\s+");

        StringBuilder filteredLine = new StringBuilder();
        for (String word : words) {
            if (!stopWords.contains(word)) {
                filteredLine.append(word).append(" ");
            }
        }

        fileNameKey.set(fileName);
        //the value will be of the form "offset,filteredLine"
        offsetLineValue.set(key.get() + "," + filteredLine.toString().trim());
        context.write(fileNameKey, offsetLineValue);
    }
}
