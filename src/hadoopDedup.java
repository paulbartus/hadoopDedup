import java.io.*;

public class hadoopDedup {

    public static void main(String[] args) throws Exception {

        String directory = "data/";

        File dataDirectory = new File(directory);
        File[] listOfFiles = dataDirectory.listFiles();

        for (File file : listOfFiles){
            if (file.isFile()){
                System.out.println(file.getPath());
                dataFile dedupFile = new dataFile(file.getPath());
                dedupFile.dedupFile();
                dedupFile.reconstructFile();
            }
        }
    }
}

