import java.io.File;
import java.util.Scanner;

public class hadoopDedup {

    public static void main(String[] args) throws Exception {

        Scanner userInputReader = new Scanner(System.in);
        System.out.print("Please enter directory to dedup: ");
        String inputDirectory = userInputReader.next();
        userInputReader.close();
        recursiveDedup(inputDirectory);
    }

    public static void recursiveDedup(String inputDirectory) throws Exception{

        File dataDirectory = new File(inputDirectory);

        if (!dataDirectory.exists()) {

            System.out.println("The directory " + inputDirectory + "/ does not exists.");

        } else {

            File[] chunks = dataDirectory.listFiles();

            for (File chunk : chunks) {

                if (chunk.isDirectory() && !chunk.isHidden()) {

                    System.out.println("All the files from " + chunk + "/ will be deduplicated.");
                    recursiveDedup(chunk.getPath());

                } else if (chunk.isFile() && !chunk.isHidden()) {

                    System.out.println(chunk.getPath());
                    hadoopChunk hadoopChunk1 = new hadoopChunk(chunk.getPath());
                    //System.out.println("Dedup");
                    long startDedup = System.currentTimeMillis();
                    hadoopChunk1.dedupHadoopChunk();
                    long endDedup = System.currentTimeMillis();
                    long dedupTime = (endDedup - startDedup);
                    System.out.println("Dedup time: " + dedupTime);
                    //System.out.println("Reconstruct");
                    long startReconst = System.currentTimeMillis();
                    hadoopChunk1.reconstructHadoopChunk();
                    long endReconst = System.currentTimeMillis();
                    long reconsTime= (endReconst - startReconst);
                    System.out.println("Reconstruction time: " + reconsTime);
                }
            }
        }
    }
}

