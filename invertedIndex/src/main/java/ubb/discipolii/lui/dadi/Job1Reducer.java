package ubb.discipolii.lui.dadi;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Job1Reducer extends Reducer<Text, Text, Text, Text> {
    private Text lineNumberKey = new Text();
    private Text fileNameLineValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // the records are of the form <fileName, "offset,filteredLine">
        List<String> lines = new ArrayList<>();
        for (Text value : values) {
            lines.add(value.toString());
        }

        // records are sorted by offset
        lines.sort((a, b) -> {
            long offsetA = Long.parseLong(a.split(",")[0]);
            long offsetB = Long.parseLong(b.split(",")[0]);
            return Long.compare(offsetA, offsetB);
        });

        int lineNumber = 1;
        for (String line : lines) {
            String[] parts = line.split(",", 2);
            lineNumberKey.set(String.valueOf(lineNumber));
            fileNameLineValue.set(key.toString() + "," + parts[1]);
            //record is of the form <lineNumber, "fileName,filteredLine">
            context.write(lineNumberKey, fileNameLineValue);
            lineNumber++;
        }
    }
}
