import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.CharBuffer;
import java.sql.*;
import java.security.MessageDigest;

public class file {

    private static String inputFileName = "data/file2.png";

    static File inputFile = new File(inputFileName);


    //Getters
    public static String getInputFileName(){

        return inputFileName;

    }

    public static long getFileLength(){

        return inputFile.length();
    }

    public static String getFileParent(){

        return inputFile.getParent();
    }

    public static long getLastChunkSize(){

        long lastChunkSize = (int) (getFileLength() % chunk.getChunkSize());
        return lastChunkSize;
    }

    public static long getNumberOfChunks(){

        long numberOfChunks = (int) (getFileLength() / chunk.getChunkSize());
        if (getLastChunkSize() > 0){

            numberOfChunks += 1;
        }
        return numberOfChunks;
    }


    // Setters
    public void setInputFileName() {

        this.inputFileName = inputFileName;

    }


        //Constructors
    public file(String inputFileName){

        this.inputFileName = inputFileName;

    }


    public static boolean checkIfExistsInDB() throws Exception, IOException, SQLException {

        Connection connectMariaDB = null;
        Statement sqlStatement = null;

        try {

            Class.forName(connectionMariaDB.getJDBC_DRIVER());

            connectMariaDB = DriverManager.getConnection(connectionMariaDB.getDB_URL(),
                    connectionMariaDB.getDB_USER(),
                    connectionMariaDB.getDB_PASSWORD());

            sqlStatement = connectMariaDB.createStatement();
            String sql_check_if_exists_file = "SELECT EXISTS (SELECT id FROM files WHERE "
                    + "name = '"
                    + inputFileName
                    + "')";

            ResultSet fileExists = sqlStatement.executeQuery(sql_check_if_exists_file);
            fileExists.next();
            boolean exists = fileExists.getBoolean(1);

            return exists;

        } finally {

            connectMariaDB.close();
            sqlStatement.close();
        }
    }

    public static String generateFileID(String inputFileName) throws Exception, IOException {

        byte[] b = Files.readAllBytes(Paths.get(inputFileName));
        byte[] fileID = MessageDigest.getInstance("MD5").digest(b);
        return md5.bytesToHex(fileID);
    }

    public static void insertIntoDB() throws Exception, IOException, SQLException {

        Connection connectMariaDB = null;
        Statement sqlStatement = null;

        try {

            Class.forName(connectionMariaDB.getJDBC_DRIVER());

            connectMariaDB = DriverManager.getConnection(connectionMariaDB.getDB_URL(),
                    connectionMariaDB.getDB_USER(),
                    connectionMariaDB.getDB_PASSWORD());

            sqlStatement = connectMariaDB.createStatement();

            String sql_insert_file = "INSERT IGNORE INTO files(id, name, size, chunksize) VALUES ('"
                    + generateFileID(inputFileName)
                    + "', '"
                    + inputFileName
                    + "', "
                    + getFileLength()
                    + ", "
                    + chunk.getChunkSize()
                    + ");";

            sqlStatement.executeQuery(sql_insert_file);

        } finally {

            connectMariaDB.close();
            sqlStatement.close();
        }
    }

    public static int dedupFile(String inputFileName){

        byte[] chunkByte = new byte[chunk.getChunkSize()];
        byte[] lastChunkByte = new byte[(int) file.getLastChunkSize()];

        Connection connectMariaDB = null;
        Statement sqlStatement = null;

        try {

            Class.forName(connectionMariaDB.getJDBC_DRIVER());

            connectMariaDB = DriverManager.getConnection( connectionMariaDB.getDB_URL(),
                    connectionMariaDB.getDB_USER(),
                    connectionMariaDB.getDB_PASSWORD());
            connectMariaDB.setAutoCommit(false);

            sqlStatement = connectMariaDB.createStatement();

            if(! checkIfExistsInDB()) {

                System.out.print("The file does not exists in the database ... ");
                insertIntoDB();
                System.out.println("Successfully added!");

                File fileDirectoryDedup = new File("dataset/" + getFileParent());
                if (!fileDirectoryDedup.exists()) {

                    fileDirectoryDedup.mkdir();
                }

                BufferedWriter fileRecipe = new BufferedWriter(new FileWriter("dataset/" + inputFileName));

                String sql_insert_chunks = "INSERT IGNORE INTO chunks(id, count, content) VALUES( ?, 1, ?)"
                        + " ON DUPLICATE KEY UPDATE count=count+1;";

                InputStream chunkStream = new FileInputStream(inputFileName);
                InputStream chunk1 = new FileInputStream(inputFileName);
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();

                long remainingChunks = getNumberOfChunks();

                PreparedStatement sql_insert_blob = null;

                while ((remainingChunks > 1 && file.getLastChunkSize() > 0) ||
                        (remainingChunks > 0 && file.getLastChunkSize() == 0)) {

                    sql_insert_blob = connectMariaDB.prepareStatement(sql_insert_chunks);
                    sql_insert_blob.setBinaryStream(2, chunkStream, 512);

                    buffer.write(chunkByte, 0, chunk1.read(chunkByte));

                    sql_insert_blob.setString(1, sha256.getSha256(buffer.toString()));
                    sql_insert_blob.executeUpdate();

                    fileRecipe.write(sha256.getSha256(buffer.toString())+"\n");
                    remainingChunks -= 1;
                }

                if (file.getLastChunkSize() > 0) {

                    sql_insert_blob = connectMariaDB.prepareStatement(sql_insert_chunks);
                    sql_insert_blob.setBinaryStream(2, chunkStream, file.getLastChunkSize());

                    buffer.write(lastChunkByte, 0, chunk1.read(lastChunkByte));

                    sql_insert_blob.setString(1, sha256.getSha256(buffer.toString()));
                    sql_insert_blob.executeUpdate();
                    fileRecipe.write(sha256.getSha256(buffer.toString()));
                }

                fileRecipe.close();
                System.out.println("File deduplicated successfully!");
            } else {

                System.out.println("File already in the database!");
            }

        } catch (Exception e) {
            if (connectMariaDB != null) {
                try {
                    sqlStatement.close();
                    connectMariaDB.rollback();
                    System.err.print("Transaction is being rolled back");
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            }


        } finally {
            if (sqlStatement != null) {
                try{
                    sqlStatement.close();
                    connectMariaDB.setAutoCommit(true);
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            }
            try {
                if (connectMariaDB != null) {
                    connectMariaDB.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        return 0;
    }



    public static int reconstructFile(String inputFileName) {

        Connection connectMariaDB = null;
        Statement sqlStatement = null;

        try {

            Class.forName(connectionMariaDB.getJDBC_DRIVER());

            connectMariaDB = DriverManager.getConnection( connectionMariaDB.getDB_URL(),
                    connectionMariaDB.getDB_USER(),
                    connectionMariaDB.getDB_PASSWORD());
            connectMariaDB.setAutoCommit(false);

            sqlStatement = connectMariaDB.createStatement();

            if(! checkIfExistsInDB()) {

                System.out.println("The requested file does not exists.");

            } else {

                System.out.print("Reconstructing ... ");

                File fileDirectoryReconstruct = new File("reconstructed/" + getFileParent());
                if (!fileDirectoryReconstruct.exists()) {

                    fileDirectoryReconstruct.mkdir();
                }

                BufferedWriter newFile = new BufferedWriter(new FileWriter("reconstructed/" + inputFileName));

                String sql_file_dedup_properties = "SELECT id, size, chunksize FROM files WHERE name='"
                        + inputFileName
                        + "';";

                ResultSet chunkProperties = sqlStatement.executeQuery(sql_file_dedup_properties);
                chunkProperties.next();

                String originalFileID = chunkProperties.getNString("id");
                long originalFileSize = chunkProperties.getInt("size");
                int originalChunkSize = chunkProperties.getInt("chunksize");

                BufferedReader fileRecipe = new BufferedReader(new FileReader("dataset/"+inputFileName));

                long totalChunks = getNumberOfChunks();
                while (totalChunks > 0) {

                    String sql_read_chunk_content = "SELECT content from chunks where id = '"
                            + fileRecipe.readLine()
                            + "' LIMIT 1;";
                    ResultSet chunkContent = sqlStatement.executeQuery(sql_read_chunk_content);
                    chunkContent.next();
                    //newFile.write(chunkContent.getBlob("content"));
                    //chunkContent.getNString("content");
                    newFile.write(chunkContent.getNString("content"));
                    //chunkContent.getBinaryStream("content");
                    //chunkContent.getBlob("content");

                    totalChunks -= 1;
                }


                fileRecipe.close();
                newFile.close();

                if ( originalFileID.compareTo(generateFileID("reconstructed/"+inputFileName)) == 0) {

                    System.out.println("File reconstructed successfully!");

                } else {

                    System.out.println("Error reconstructing the file!");
                }

            }

        } catch (Exception e) {
            if (connectMariaDB != null) {
                try {
                    sqlStatement.close();
                    connectMariaDB.rollback();
                    System.err.print("Transaction is being rolled back");
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            }


        } finally {
            if (sqlStatement != null) {
                try{
                    sqlStatement.close();
                    connectMariaDB.setAutoCommit(true);
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            }
            try {
                if (connectMariaDB != null) {
                    connectMariaDB.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        return 0;
    }

}
