import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class hadoopChunkWithDedup extends hadoopChunk {

    //Constructors
    public hadoopChunkWithDedup(String inputFileName) {

        super(inputFileName);
        this.inputFileName = inputFileName;
    }

    public void dedupHadoopChunk() throws Exception {

        Connection connectMariaDB = null;
        PreparedStatement sql_insert_blob = null;

        try {

            connectMariaDB = connectionMariaDB.getDBConnection();
            connectMariaDB.setAutoCommit(false);

            String sql_insert_chunks = "INSERT IGNORE INTO chunks(id, count, content) VALUES( ?, 1, ?)"
                    + " ON DUPLICATE KEY UPDATE count=count+1;";

            if (!checkIfExistsInDB()) {

                System.out.print("The file does not exists in the database ... ");

                //File fileDirectoryDedup = new File("filerecipes/" + getFileParent());
                File fileDirectoryDedup = new File(getFileParent());
                if (!fileDirectoryDedup.exists()) {

                    fileDirectoryDedup.mkdirs();
                }

                //BufferedWriter fileRecipe = new BufferedWriter(new FileWriter("filerecipes/" + inputFileName ));
                BufferedWriter fileRecipe = new BufferedWriter(new FileWriter(inputFileName + ".fr" ));

                InputStream chunkingStream = new FileInputStream(inputFileName);
                InputStream chunkingStreamToHash = new FileInputStream(inputFileName);

                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                sql_insert_blob = connectMariaDB.prepareStatement(sql_insert_chunks);

                long remainingChunks = getNumberOfChunks();
                long lastChunkSize = getLastChunkSize();

                while ((remainingChunks > 1 && lastChunkSize > 0) || (remainingChunks > 0 && lastChunkSize == 0)) {

                    sql_insert_blob.setBinaryStream(2, chunkingStream, dedupChunk.getChunkSize());
                    buffer.write(dedupChunk.getChunkContent(), 0, chunkingStreamToHash.read(dedupChunk.getChunkContent()));
                    dedupChunk.setChunkID();
                    sql_insert_blob.setString(1, dedupChunk.getChunkID());
                    sql_insert_blob.executeUpdate();  // 85 - 90 % of running time
                    fileRecipe.write(dedupChunk.getChunkID() + "\n");
                    remainingChunks -= 1;
                }

                if (lastChunkSize > 0) {

                    sql_insert_blob.setBinaryStream(2, chunkingStream, lastChunkSize);
                    buffer.write(lastChunk.getChunkContent(), 0, chunkingStreamToHash.read(lastChunk.getChunkContent()));
                    lastChunk.setChunkID();
                    sql_insert_blob.setString(1, lastChunk.getChunkID());
                    sql_insert_blob.executeUpdate();
                    fileRecipe.write(lastChunk.getChunkID());
                }

                connectMariaDB.commit();
                fileRecipe.close();
                insertIntoDB();
                System.out.println("Successfully added NEW!");

            } else {
                connectMariaDB.rollback();
                System.out.println("File already in the database!");
            }

        } catch (SQLException sqlException1) {
            try {
                if(connectMariaDB != null)
                    connectMariaDB.rollback();
            } catch (SQLException sqlException2) {
                System.out.println(sqlException2.getMessage());
            }
            System.out.println(sqlException1.getMessage());

        } finally {
            try {
                if (sql_insert_blob != null) {
                    sql_insert_blob.close();
                }
                if (connectMariaDB != null) {
                    connectionMariaDB.closeDBConnection(connectMariaDB);
                }
            } catch (SQLException sqlException3) {
                System.out.println(sqlException3.getMessage());
            }
        }
    }
}
