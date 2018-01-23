import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;

public class hadoopChunk extends File{

    private String inputFileName;

    dedupChunk dedupChunk = new dedupChunk(512,false);

    connectionMariaDB connectionMariaDB = new connectionMariaDB();

    //Getters
    public String getInputFileName(){

        return inputFileName;
    }

    public long getFileLength(){

        return length();
    }

    public String getFileParent(){

        return getParent();
    }

    public int getLastChunkSize(){

        int lastChunkSize = (int) (getFileLength() % dedupChunk.getChunkSize());
        return lastChunkSize;
    }

    dedupChunk lastChunk = new dedupChunk(getLastChunkSize(),true);

    public long getNumberOfChunks(){

        long numberOfChunks = (int) (getFileLength() / dedupChunk.getChunkSize());
        if (getLastChunkSize() > 0){

            numberOfChunks += 1;
        }
        return numberOfChunks;
    }

    //Constructors
    public hadoopChunk(String inputFileName) {

        super(inputFileName);
        this.inputFileName = inputFileName;
    }

    public boolean checkIfExistsInDB() throws Exception {

        String sql_check_if_exists_file = "SELECT EXISTS (SELECT id FROM files WHERE "
                + "name = '"
                + inputFileName
                + "')";

        Connection connectMariaDB = connectionMariaDB.getDBConnection();

        Statement sqlStatement = connectMariaDB.createStatement();

        try{

            ResultSet fileExists = sqlStatement.executeQuery(sql_check_if_exists_file);
            fileExists.next();
            boolean exists = fileExists.getBoolean(1);
            return exists;

        } finally {
            connectionMariaDB.closeDBConnection(connectMariaDB);
            sqlStatement.close();
        }
    }

    public String generateFileID() throws Exception {

        String fileID = DigestUtils.md5Hex(Files.readAllBytes(Paths.get(inputFileName)));
        return fileID;
    }

    public void insertIntoDB() throws Exception {

        String sql_insert_file = "INSERT IGNORE INTO files(id, name, size, chunksize) VALUES ('"
                + generateFileID()
                + "', '"
                + inputFileName
                + "', "
                + getFileLength()
                + ", "
                + dedupChunk.getChunkSize()
                + ");";

        Connection connectMariaDB = connectionMariaDB.getDBConnection();

        Statement sqlStatement = connectMariaDB.createStatement();

        try{

            sqlStatement.executeQuery(sql_insert_file);

        } finally {

            connectionMariaDB.closeDBConnection(connectMariaDB);
            sqlStatement.close();
        }
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
                System.out.println("Successfully added!");

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


    public hadoopChunk reconstructHadoopChunk() throws Exception {

        //hadoopChunk reconstructedHadoopChunk = new hadoopChunk("reconstructed/" + inputFileName);
        hadoopChunk reconstructedHadoopChunk = new hadoopChunk(inputFileName);

        String sql_file_dedup_properties = "SELECT id, size, chunksize FROM files WHERE name='"
                + inputFileName
                + "';";

        Connection connectMariaDB = connectionMariaDB.getDBConnection();

        Statement sqlStatement = connectMariaDB.createStatement();

        try{
            if(! checkIfExistsInDB()) {

                System.out.println("The requested file does not exists.");

            } else {

                System.out.print("Reconstructing ... ");

                //File fileDirectoryReconstruct = new File("reconstructed/" + getFileParent());
                File fileDirectoryReconstruct = new File(getFileParent());

                if (!fileDirectoryReconstruct.exists()) {

                    fileDirectoryReconstruct.mkdirs();
                }

                FileOutputStream newFileOutputStream = new FileOutputStream(reconstructedHadoopChunk);

                ResultSet chunkProperties = sqlStatement.executeQuery(sql_file_dedup_properties);
                chunkProperties.next();

                //BufferedReader fileRecipe = new BufferedReader(new FileReader("filerecipes/" + inputFileName));
                BufferedReader fileRecipe = new BufferedReader(new FileReader(inputFileName + ".fr"));

                String originalFileID = chunkProperties.getNString("id");
                long originalFileSize = chunkProperties.getInt("size");
                int originalChunkSize = chunkProperties.getInt("chunksize");
                int lastChunkSize = (int) originalFileSize % originalChunkSize;
                long totalChunks = (int) originalFileSize / originalChunkSize;

                if (lastChunkSize > 0 ){
                    totalChunks += 1;
                }

                byte[] chunkByte = new byte[originalChunkSize];
                byte[] lastChunkByte = new byte[lastChunkSize];

                while ((totalChunks > 1 && lastChunkSize > 0) || (totalChunks > 0 && lastChunkSize == 0)) {

                    String sql_read_chunk_content = "SELECT content from chunks where id = '"
                            + fileRecipe.readLine()
                            + "' LIMIT 1;";
                    PreparedStatement statement = connectMariaDB.prepareStatement(sql_read_chunk_content);
                    ResultSet chunkContent = statement.executeQuery(); // 85 - 90 % of running time
                    chunkContent.next();
                    InputStream stream = chunkContent.getBinaryStream("content");

                    while (stream.read(chunkByte) > 0){
                        newFileOutputStream.write(chunkByte);
                    }

                    totalChunks -= 1;
                }

                if (lastChunkSize > 0) {
                    String sql_read_chunk_content = "SELECT content from chunks where id = '"
                            + fileRecipe.readLine()
                            + "' LIMIT 1;";
                    PreparedStatement statement = connectMariaDB.prepareStatement(sql_read_chunk_content);
                    ResultSet chunkContent = statement.executeQuery();
                    chunkContent.next();
                    InputStream stream = chunkContent.getBinaryStream("content");
                    while (stream.read(lastChunkByte) > 0){
                        newFileOutputStream.write(lastChunkByte);
                    }
                }

                newFileOutputStream.close();
                fileRecipe.close();

                if ( originalFileID.compareTo(reconstructedHadoopChunk.generateFileID()) == 0) {

                    System.out.println("File reconstructed successfully!");

                } else {

                    System.out.println("Error reconstructing the file!");
                }
            }
        }

        catch (Exception exception) {
            System.out.println(exception.getMessage());

        } finally {
            try {
                if (sqlStatement != null) {
                    sqlStatement.close();
                }
                if (connectMariaDB != null) {
                    connectionMariaDB.closeDBConnection(connectMariaDB);
                }
            } catch (SQLException sqlException) {
                System.out.println(sqlException.getMessage());
            }
        }

    return reconstructedHadoopChunk;

    }
}