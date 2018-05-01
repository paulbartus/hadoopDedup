import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;

public class hadoopChunk extends File{
    protected String inputFileName;
    dedupChunk dedupChunk = new dedupChunk(512);
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
        String sql_check_if_exists_file = "SELECT EXISTS (SELECT fileId FROM file WHERE "
                + "fileName = '"
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

    public long computeFileLength() throws Exception {
            Connection connectMariaDB = connectionMariaDB.getDBConnection();
            Statement sqlStatement = connectMariaDB.createStatement();
            InputStream in = new FileInputStream(new File(inputFileName + ".fr"));
            try {
                long fileLength = 0;
                int numBytes;
                byte buf[] = new byte[64];
                int bytesRead = in.read(buf);
                String chunkId = new String(buf);
                while (bytesRead > 0) {
                    String sql_read_chunk_numBytes = "SELECT numBytes from chunk where chunkId = '"
                            + chunkId
                            + "' LIMIT 1;";
                    ResultSet storedChunkSize = sqlStatement.executeQuery(sql_read_chunk_numBytes);
                    storedChunkSize.next();
                    numBytes = storedChunkSize.getInt("numBytes");
                    fileLength += numBytes;
                    bytesRead = in.read(buf);
                    chunkId = new String(buf);
                }
                in.close();
                return fileLength;
            }
             finally{
                    connectionMariaDB.closeDBConnection(connectMariaDB);
                    sqlStatement.close();
                    }
    }

    public void insertIntoDB() throws Exception {
        String sql_insert_file = "INSERT IGNORE INTO file(fileId, fileName, fileSize) VALUES ('"
                + generateFileID()
                + "', '"
                + inputFileName
                + "', "
                + getFileLength()
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
            String sql_insert_chunk_content = "INSERT IGNORE INTO chunk(chunkId, numBytes, count, content) VALUES( ?, ?, 1, ?)"
                    + " ON DUPLICATE KEY UPDATE count=count+1;";
            if (!checkIfExistsInDB()) {
                System.out.print("The file does not exists in the database ... ");
                File fileDirectoryDedup = new File(getFileParent());
                if (!fileDirectoryDedup.exists()) {
                    fileDirectoryDedup.mkdirs();
                }
                BufferedWriter fileRecipe = new BufferedWriter(new FileWriter(inputFileName + ".fr" ));
                InputStream in = new FileInputStream(inputFileName);
                sql_insert_blob = connectMariaDB.prepareStatement(sql_insert_chunk_content);
                int chunkSize = dedupChunk.getChunkSize();
                byte chunk[] = new byte[chunkSize];
                int bytesToChunk = in.read(chunk);
                while (bytesToChunk > 0) {
                    int bytesToChunkNext = (chunkSize < bytesToChunk) ? chunkSize : (int) bytesToChunk;
                    String chunkIdString = DigestUtils.sha256Hex(chunk);   // compute sha256 for the current chunk content
                    sql_insert_blob = connectMariaDB.prepareStatement(sql_insert_chunk_content);
                    sql_insert_blob.setString(1, chunkIdString);
                    sql_insert_blob.setInt(2, bytesToChunkNext);
                    sql_insert_blob.setBinaryStream(3, new ByteArrayInputStream(chunk), bytesToChunkNext);
                    sql_insert_blob.executeUpdate();
                    fileRecipe.write(chunkIdString);
                    bytesToChunk = in.read(chunk);
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
        hadoopChunk reconstructedHadoopChunk = new hadoopChunk(inputFileName);
        String sql_file_dedup_properties = "SELECT fileId FROM file WHERE fileName='"
                + inputFileName
                + "';";
        Connection connectMariaDB = connectionMariaDB.getDBConnection();
        Statement sqlStatement = connectMariaDB.createStatement();
        try{
            if(! checkIfExistsInDB()) {
                System.out.println("The requested file does not exists.");
            } else {
                System.out.print("Reconstructing ... ");
                File fileDirectoryReconstruct = new File(getFileParent());
                if (!fileDirectoryReconstruct.exists()) {
                    fileDirectoryReconstruct.mkdirs();
                }
                FileOutputStream out = new FileOutputStream(reconstructedHadoopChunk);
                ResultSet chunkProperties = sqlStatement.executeQuery(sql_file_dedup_properties);
                chunkProperties.next();
                InputStream in = new FileInputStream(new File(inputFileName + ".fr"));
                String originalFileID = chunkProperties.getNString("fileId");
                byte buf[] = new byte[64];
                int bytesRead = in.read(buf);
                String chunkId = new String(buf);
                while (bytesRead >0){
                    String sql_read_chunk_content = "SELECT content, numBytes from chunk where chunkId = '"
                            + chunkId
                            + "' LIMIT 1;";
                    PreparedStatement statement = connectMariaDB.prepareStatement(sql_read_chunk_content);
                    ResultSet chunkContentAndSize = statement.executeQuery(); // 85 - 90 % of running time
                    chunkContentAndSize.next();
                    InputStream inp = chunkContentAndSize.getBinaryStream("content");
                    int chunkSize = chunkContentAndSize.getInt("numBytes");
                    byte[] chunkByte = new byte[chunkSize];
                    while (inp.read(chunkByte) >= 0) {
                        out.write(chunkByte);
                    }
                    bytesRead = in.read(buf);
                    chunkId = new String(buf);
                }
                out.close();
                in.close();
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