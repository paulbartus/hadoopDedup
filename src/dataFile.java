import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.security.MessageDigest;

public class dataFile extends File{

    private String inputFileName;

    chunk dedupChunk = new chunk(512);

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

        long lastChunkSize = (int) (getFileLength() % dedupChunk.getChunkSize());
        return lastChunkSize;
    }

    public long getNumberOfChunks(){

        long numberOfChunks = (int) (getFileLength() / dedupChunk.getChunkSize());
        if (getLastChunkSize() > 0){

            numberOfChunks += 1;
        }
        return numberOfChunks;
    }

    // Setters
    public void setInputFileName(String inputFileName) {

        this.inputFileName = inputFileName;
    }

    //Constructors
    public dataFile(String inputFileName) {

        super(inputFileName);
        this.inputFileName = inputFileName;
        File inputFile = new File(inputFileName);
    }


    public boolean checkIfExistsInDB() throws Exception, IOException, SQLException {

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

    public String generateFileID(String inputFileName) throws Exception, IOException {

        byte[] b = Files.readAllBytes(Paths.get(inputFileName));
        byte[] fileID = MessageDigest.getInstance("MD5").digest(b);
        return md5.bytesToHex(fileID);
    }

    public void insertIntoDB() throws Exception, IOException, SQLException {

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
                    + dedupChunk.getChunkSize()
                    + ");";

            sqlStatement.executeQuery(sql_insert_file);

        } finally {

            connectMariaDB.close();
            sqlStatement.close();
        }
    }

    public int dedupFile(){

        byte[] chunkByte = new byte[dedupChunk.getChunkSize()];
        byte[] lastChunkByte = new byte[(int) getLastChunkSize()];

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
                long lastChunkSize = getLastChunkSize();

                PreparedStatement sql_insert_blob = null;

                while ((remainingChunks > 1 && lastChunkSize > 0) ||
                        (remainingChunks > 0 && lastChunkSize == 0)) {

                    sql_insert_blob = connectMariaDB.prepareStatement(sql_insert_chunks);
                    sql_insert_blob.setBinaryStream(2, chunkStream, dedupChunk.getChunkSize());

                    buffer.write(chunkByte, 0, chunk1.read(chunkByte));

                    sql_insert_blob.setString(1, sha256.getSha256(buffer.toString()));
                    sql_insert_blob.executeUpdate();

                    fileRecipe.write(sha256.getSha256(buffer.toString())+"\n");
                    remainingChunks -= 1;
                }

                if (lastChunkSize > 0) {

                    sql_insert_blob = connectMariaDB.prepareStatement(sql_insert_chunks);
                    sql_insert_blob.setBinaryStream(2, chunkStream, lastChunkSize);

                    buffer.write(lastChunkByte, 0, chunk1.read(lastChunkByte));

                    sql_insert_blob.setString(1, sha256.getSha256(buffer.toString()));
                    sql_insert_blob.executeUpdate();
                    fileRecipe.write(sha256.getSha256(buffer.toString()));
                }

                fileRecipe.close();
                System.out.println("Successfully added!");
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


    public int reconstructFile() {

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

                //BufferedWriter newFile = new BufferedWriter(new FileWriter("reconstructed/" + inputFileName));
                File newFile = new File("reconstructed/" + inputFileName);
                FileOutputStream newFileOutputStream = new FileOutputStream(newFile);

                String sql_file_dedup_properties = "SELECT id, size, chunksize FROM files WHERE name='"
                        + inputFileName
                        + "';";

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

