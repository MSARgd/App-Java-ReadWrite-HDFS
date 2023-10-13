package ma.enset;

/**
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
public class AppRead {
    public static void main(String[] args) throws IOException {
        Configuration configration = new Configuration();
        configration.set("fs.defaultFs","hdfs://localhost:9870");
        FileSystem fileSystem = FileSystem.get(configration);
//        Path path = new Path( "/home/msa/text.txt");
        Path path = new Path( "/BDCC/CPP/Cours/CoursCPP1");
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        BufferedReader bufferedReader = new BufferedReader(new
                InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8));
        String line = null;
        while ((line=bufferedReader.readLine())!=null){
            System.out.println(line);
        }
        fsDataInputStream.close();
        fileSystem.close();
    }
}
 **/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.UUID;

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
