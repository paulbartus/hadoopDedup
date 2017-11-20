import java.io.*;
import java.util.Scanner;

public class hadoopDedup {

    public static void main(String[] args) throws Exception {

        Scanner userInputReader = new Scanner(System.in);
        System.out.print("Please enter directory to dedup: ");
        String inputDirectory = userInputReader.next();
        userInputReader.close();

        File dataDirectory = new File(inputDirectory);
        if (!dataDirectory.exists()) {

            System.out.println("The directory " + inputDirectory + "/ does not exists.");

        } else {

            System.out.println("All the files from " + inputDirectory + "/ will be deduplicated.");
            File[] listOfFiles = dataDirectory.listFiles();

            for (File file : listOfFiles) {
                if (file.isFile()) {
                    System.out.println(file.getPath());
                    dataFile dedupFile = new dataFile(file.getPath());
                    System.out.println("Dedup");
                    dedupFile.dedupFile();
                    System.out.println("Reconstruct");
                    dedupFile.reconstructFile();
                }
            }
        }
    }
}

