import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;

public class dataFile extends File{

    private String inputFileName;

    chunk dedupChunk = new chunk();
    int chunkSize = dedupChunk.getChunkSize();

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

    public long getLastChunkSize(){

        long lastChunkSize = (int) (getFileLength() % chunkSize);
        return lastChunkSize;
    }

    public long getNumberOfChunks(){

        long numberOfChunks = (int) (getFileLength() / chunkSize);
        if (getLastChunkSize() > 0){

            numberOfChunks += 1;
        }
        return numberOfChunks;
    }

    //Constructors
    public dataFile(String inputFileName) {

        super(inputFileName);
        this.inputFileName = inputFileName;
        File inputFile = new File(inputFileName);
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

    public String generateFileID(String inputFileName) throws Exception {

        String fileID = DigestUtils.md5Hex(Files.readAllBytes(Paths.get(inputFileName)));
        return fileID;
    }

    public void insertIntoDB() throws Exception {

        String sql_insert_file = "INSERT IGNORE INTO files(id, name, size, chunksize) VALUES ('"
                + generateFileID(inputFileName)
                + "', '"
                + inputFileName
                + "', "
                + getFileLength()
                + ", "
                + chunkSize
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

    public void dedupFile() throws Exception {

        byte[] chunkByte = new byte[chunkSize];
        byte[] lastChunkByte = new byte[(int) getLastChunkSize()];

        Connection connectMariaDB = null;
        PreparedStatement sql_insert_blob = null;

        try {

            connectMariaDB = connectionMariaDB.getDBConnection();

            connectMariaDB.setAutoCommit(false);

            String sql_insert_chunks = "INSERT IGNORE INTO chunks(id, count, content) VALUES( ?, 1, ?)"
                    + " ON DUPLICATE KEY UPDATE count=count+1;";

            if (!checkIfExistsInDB()) {

                System.out.print("The file does not exists in the database ... ");

                File fileDirectoryDedup = new File("dataset/" + getFileParent());
                if (!fileDirectoryDedup.exists()) {

                    fileDirectoryDedup.mkdir();
                }

                BufferedWriter fileRecipe = new BufferedWriter(new FileWriter("dataset/" + inputFileName));

                InputStream chunkStream = new FileInputStream(inputFileName);
                InputStream chunkStreamToHash = new FileInputStream(inputFileName);

                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                sql_insert_blob = connectMariaDB.prepareStatement(sql_insert_chunks);

                long remainingChunks = getNumberOfChunks();
                long lastChunkSize = getLastChunkSize();

                while ((remainingChunks > 1 && lastChunkSize > 0) || (remainingChunks > 0 && lastChunkSize == 0)) {

                    sql_insert_blob.setBinaryStream(2, chunkStream, chunkSize);
                    buffer.write(chunkByte, 0, chunkStreamToHash.read(chunkByte));
                    dedupChunk.generateChunkID(chunkByte);

                    //dedupChunk.setChunkID(DigestUtils.sha256Hex(chunkByte));
                    sql_insert_blob.setString(1, dedupChunk.getChunkID());
                    sql_insert_blob.executeUpdate();
                    fileRecipe.write(dedupChunk.getChunkID() + "\n");
                    remainingChunks -= 1;
                }

                if (lastChunkSize > 0) {

                    sql_insert_blob.setBinaryStream(2, chunkStream, lastChunkSize);
                    buffer.write(lastChunkByte, 0, chunkStreamToHash.read(lastChunkByte));
                    dedupChunk.generateChunkID(lastChunkByte);
                    sql_insert_blob.setString(1, dedupChunk.getChunkID());
                    sql_insert_blob.executeUpdate();
                    fileRecipe.write(dedupChunk.getChunkID());
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


    public void reconstructFile() throws Exception {

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

                File fileDirectoryReconstruct = new File("reconstructed/" + getFileParent());
                if (!fileDirectoryReconstruct.exists()) {

                    fileDirectoryReconstruct.mkdir();
                }

                File newFile = new File("reconstructed/" + inputFileName);
                FileOutputStream newFileOutputStream = new FileOutputStream(newFile);

                ResultSet chunkProperties = sqlStatement.executeQuery(sql_file_dedup_properties);
                chunkProperties.next();

                BufferedReader fileRecipe = new BufferedReader(new FileReader("dataset/"+inputFileName));

                String originalFileID = chunkProperties.getNString("id");
                long originalFileSize = chunkProperties.getInt("size");
                int originalChunkSize = chunkProperties.getInt("chunksize");
                long lastChunkSize = (int) originalFileSize % originalChunkSize;
                long totalChunks = (int) originalFileSize / originalChunkSize;

                if (lastChunkSize > 0 ){
                    totalChunks += 1;
                }

                while (totalChunks > 0) {

                    String sql_read_chunk_content = "SELECT content from chunks where id = '"
                            + fileRecipe.readLine()
                            + "' LIMIT 1;";
                    PreparedStatement statement = connectMariaDB.prepareStatement(sql_read_chunk_content);

                    ResultSet chunkContent = statement.executeQuery();

                    chunkContent.next();
                    InputStream stream = chunkContent.getBinaryStream("content");
                    byte[] buffer;
                    if (totalChunks > 1) {
                        buffer = new byte[originalChunkSize];
                    } else {
                        buffer = new byte[(int) lastChunkSize];
                    }
                    while (stream.read(buffer) > 0){
                        newFileOutputStream.write(buffer);
                    }

                    totalChunks -= 1;
                }

                newFileOutputStream.close();
                fileRecipe.close();

                if ( originalFileID.compareTo(generateFileID("reconstructed/"+inputFileName)) == 0) {

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
    }
}