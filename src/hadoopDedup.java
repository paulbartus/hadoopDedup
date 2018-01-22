import java.io.File;

public class hadoopDedup {


    public static void main(String[] args) throws Exception {


        //Scanner userInputReader = new Scanner(System.in);
        //System.out.print("Please enter directory to dedup: ");
        //String inputDirectory = userInputReader.next();
        //userInputReader.close();

        String hadoopChunksFinalizedDirectory = "datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized";
        recursiveDedup(hadoopChunksFinalizedDirectory);
    }

    public static void recursiveDedup(String hadoopChunksDirectory) throws Exception{

        File hadoopChunksDir = new File(hadoopChunksDirectory);

        if (!hadoopChunksDir.exists()) {

            System.out.println("The directory " + hadoopChunksDir + "/ does not exists.");

        } else {

            File[] hadoopChunks = hadoopChunksDir.listFiles();

            for (File hadoopChunkFileOrDir : hadoopChunks) {

                if (hadoopChunkFileOrDir.isDirectory() && !hadoopChunkFileOrDir.isHidden()) {

                    System.out.println("All the files from " + hadoopChunkFileOrDir + "/ will be deduplicated.");
                    recursiveDedup(hadoopChunkFileOrDir.getPath());

                } else if (hadoopChunkFileOrDir.isFile() && !hadoopChunkFileOrDir.isHidden()
                                                         && !hadoopChunkFileOrDir.getName().endsWith(".meta")) {

                    System.out.println(hadoopChunkFileOrDir.getPath());
                    hadoopChunk theChunk = new hadoopChunk(hadoopChunkFileOrDir.getPath());
                    //System.out.println("Dedup");
                    long startDedup = System.currentTimeMillis();
                    theChunk.dedupHadoopChunk();
                    //hadoopChunkFileOrDir.delete();
                    long endDedup = System.currentTimeMillis();
                    long dedupTime = (endDedup - startDedup);
                    System.out.println("Dedup time: " + dedupTime);
                    //System.out.println("Reconstruct");
                    long startReconst = System.currentTimeMillis();
                    theChunk.reconstructHadoopChunk();
                    long endReconst = System.currentTimeMillis();
                    long reconsTime= (endReconst - startReconst);
                    System.out.println("Reconstruction time: " + reconsTime);
                }
            }
        }
    }
}

