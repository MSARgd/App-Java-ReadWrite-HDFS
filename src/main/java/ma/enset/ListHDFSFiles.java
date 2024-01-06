package ma.enset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.IOException;
public class ListHDFSFiles {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFs", "hdfs://localhost:9000");
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            Path directoryPath = new Path("/path/to/directory");
            FileStatus[] fileStatuses = fileSystem.listStatus(directoryPath);
            for (FileStatus status : fileStatuses) {
                System.out.println("File: " + status.getPath() +
                        " | Size: " + status.getLen() +
                        " | Modification Time: " + status.getModificationTime());
            }
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}