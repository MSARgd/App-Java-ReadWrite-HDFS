package ma.enset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class AppRead {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        // HDFS file path
        String filePath = "/BDCC/CPP/Cours/CoursCPP1"; // Modify the path as needed
        try {
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(filePath);

            // Open an input stream to the HDFS file
            FSDataInputStream inputStream = fs.open(path);

            // Wrap the input stream with a BufferedReader to read lines
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            System.out.println("Contents of the file:");
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            // Close the input stream and reader
            reader.close();
            inputStream.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
