package ubb.discipolii.lui.dadi;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Job2Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // the records are of the form <word, "fileName,lineNumber">
        Map<String, StringBuilder> fileLinesMap = new HashMap<>();

        for (Text value : values) {
            String[] parts = value.toString().split(",", 2);
            String fileName = parts[0];
            String lineNumber = parts[1];

            fileLinesMap.putIfAbsent(fileName, new StringBuilder());
            fileLinesMap.get(fileName).append(lineNumber).append(" ");
        }

        StringBuilder invertedIndex = new StringBuilder();
        for (Map.Entry<String, StringBuilder> entry : fileLinesMap.entrySet()) {
            invertedIndex.append("(").append(entry.getKey()).append(",").append(entry.getValue().toString().trim()).append(") ");
        }

        context.write(key, new Text(invertedIndex.toString().trim()));
    }
}

