package ma.enset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
public class AppWrite {

    public static void main(String[] args) {
        // HDFS configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        String filePath = "/BDCC/CPP/Cours/CoursCPP1";
        try {
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(filePath);
            FSDataOutputStream outputStream = fs.create(path);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            for (int i = 0; i < 10; i++) {
                writer.write(UUID.randomUUID().toString().substring(0, 30));
                writer.newLine();  // Write a new line after each UUID
            }
            writer.close();
            outputStream.close();
            System.out.println("Data Saved");
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }
}