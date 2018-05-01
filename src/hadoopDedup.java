import java.io.File;

public class hadoopDedup {

    public static void main(String[] args) throws Exception {
        String hadoopChunksFinalizedDirectory = "datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized";
        //String hadoopChunksFinalizedDirectory = "data";
        recursiveDedupFinalizedDirectory(hadoopChunksFinalizedDirectory);
    }

    public static void recursiveDedupFinalizedDirectory(String hadoopChunksDirectory) throws Exception{
        File hadoopChunksFinalizedDirectoryContent = new File(hadoopChunksDirectory);

        if (!hadoopChunksFinalizedDirectoryContent.exists()) {

            System.out.println("The directory " + hadoopChunksFinalizedDirectoryContent + " does not exists.");

        } else {

            File[] hadoopChunksDirectoryContent = hadoopChunksFinalizedDirectoryContent.listFiles();

            for (File hadoopChunkDirectoryItem : hadoopChunksDirectoryContent) {

                if (hadoopChunkDirectoryItem.isDirectory() && !hadoopChunkDirectoryItem.isHidden()) {

                    System.out.println("All the files from " + hadoopChunkDirectoryItem + " will be deduplicated.");
                    recursiveDedupFinalizedDirectory(hadoopChunkDirectoryItem.getPath());

                } else if (hadoopChunkDirectoryItem.isFile() && !hadoopChunkDirectoryItem.isHidden()
                                                         && !hadoopChunkDirectoryItem.getName().endsWith(".meta")
                                                         && !hadoopChunkDirectoryItem.getName().endsWith(".fr")) {

                    System.out.println(hadoopChunkDirectoryItem.getPath());
                    hadoopChunk theChunk = new hadoopChunk(hadoopChunkDirectoryItem.getPath());
                    long startDedup = System.currentTimeMillis();
                    theChunk.dedupHadoopChunk();
                    long endDedup = System.currentTimeMillis();
                    hadoopChunkDirectoryItem.delete();
                    long dedupTime = endDedup - startDedup;
                    System.out.println("Current dedup time: " + dedupTime);
                    long startReconst = System.currentTimeMillis();
                    theChunk.reconstructHadoopChunk();
                    long endReconst = System.currentTimeMillis();
                    long reconsTime= endReconst - startReconst;
                    System.out.println("Reconstruction time: " + reconsTime);
                }
            }

        }
    }
}

