package ubb.discipolii.lui.dadi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text wordKey = new Text();
    private Text fileNameLineNumberValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //record is of the form <lineNumber, "fileName,filteredLine">
        String[] parts = value.toString().split(",", 3);
        String lineNumber = parts[0];
        String fileName = parts[1];
        String lineRead = parts[2];

        String[] words = lineRead.split("\\s+");
        for (String word : words) {
            wordKey.set(word);
            fileNameLineNumberValue.set(fileName + "," + lineNumber);
            context.write(wordKey, fileNameLineNumberValue);
        }
    }
}

