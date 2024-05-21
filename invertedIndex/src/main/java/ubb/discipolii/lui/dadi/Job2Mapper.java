package ubb.discipolii.lui.dadi;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job2Mapper extends Mapper<Text, Text, Text, Text> {
    private Text wordKey = new Text();
    private Text fileNameLineNumberValue = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //record is of the form <lineNumber, "fileName,filteredLine"> where the key is separated by a tab character from the value
        // Key is the line number
        String lineNumber = key.toString();
        // Value is "fileName,filteredLine"
        String[] parts = value.toString().split(",", 2);
        String fileName = parts[0];
        String lineRead = parts[1];

        String[] words = lineRead.split("\\s+");
        for (String word : words) {
            wordKey.set(word);
            fileNameLineNumberValue.set(fileName + "," + lineNumber);
            context.write(wordKey, fileNameLineNumberValue);
        }
    }
}

