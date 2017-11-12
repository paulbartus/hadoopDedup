import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.security.MessageDigest;
import java.util.*;

public class connectMariaDB {
    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "jdbc:mariadb://localhost/ext4";
    static final String DB_USER = "root";
    static final String DB_PASSWORD = "54h5dynKPf";
    private static int chunkSize = 512;


    public static void main(String[] args) {
        //if (args.length < 1) {
        //    System.out.println("Input file not given.");
        //    System.exit(0);
        //}
        //String inputFile = args[0];

        String inputFileName = "data/file1.png";
        File inputFile = new File(inputFileName);
        byte[] chunkByte = new byte[chunkSize];

        Connection conn = null;
        Statement stmt = null;

        try {

            Class.forName("org.mariadb.jdbc.Driver");
            System.out.print("Connecting to the database...");
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            conn.setAutoCommit(false);

            System.out.println(" Successfully.");

            System.out.print("Creating the tables files and chunks if not exist in the given database...");
            stmt = conn.createStatement();

            String sql_create_table_files = "CREATE TABLE IF NOT EXISTS files "
                    + "(id CHAR (32) NOT NULL, "
                    + "name CHAR (200) NOT NULL, "
                    + "size INT NOT NULL, "
                    + "chunksize INT NOT NULL, "
                    + "PRIMARY KEY (id , name )) ENGINE = ARIA";

            stmt.executeUpdate(sql_create_table_files);

            String sql_create_table_chunks = "CREATE TABLE IF NOT EXISTS chunks "
                    + "(id CHAR (64) PRIMARY KEY NOT NULL, "
                    + "count INT, "
                    + "content BLOB ) ENGINE = ARIA";

            stmt.executeUpdate(sql_create_table_chunks);
            System.out.println(" Successfully.");
            System.out.print("Checking if the file exists in the given database... ");

            String sql_check_if_exists_file = "SELECT EXISTS (SELECT id FROM files WHERE "
                    + "name = '"
                    + inputFileName
                    + "')";

            ResultSet fileExists = stmt.executeQuery(sql_check_if_exists_file);
            fileExists.next();
            boolean exists = fileExists.getBoolean(1);

            if (exists) {

                System.out.println("The file already exists!");
            }
            else {

                System.out.println("The file does not exists!");
            }

            byte[] b = Files.readAllBytes(Paths.get(inputFileName));
            byte[] fileID = MessageDigest.getInstance("MD5").digest(b);

            String sql_insert_file = "INSERT IGNORE INTO files(id, name, size, chunksize) VALUES ('"
                    + md5.bytesToHex(fileID)
                    + "', '"
                    + inputFileName
                    + "', "
                    + inputFile.length()
                    + ", "
                    + chunkSize
                    + ");";

            stmt.executeQuery(sql_insert_file);
            long numberOfChunks = (int) (inputFile.length() / chunkSize);
            long lastChunkSize = (int) (inputFile.length() % chunkSize);
            if (lastChunkSize > 0)
                numberOfChunks += 1;
            byte[] lastChunkByte = new byte[(int) lastChunkSize];

            File fileDirectory = new File("dataset/"+inputFile.getParent());
            if (! fileDirectory.exists()) {

                fileDirectory.mkdir();
            }

            BufferedWriter fileRecipe = new BufferedWriter(new FileWriter("dataset/"+inputFileName));

            String sql_insert_chunks = "INSERT IGNORE INTO chunks(id, count, content) VALUES( ?, 1, ?)"
                    + " ON DUPLICATE KEY UPDATE count=count+1;";

            InputStream chunk = new FileInputStream(inputFileName);
            InputStream chunk1 = new FileInputStream(inputFileName);
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            while ((numberOfChunks > 1 && lastChunkSize > 0) || (numberOfChunks > 0 && lastChunkSize == 0)){

                PreparedStatement sql_insert_blob = null;
                sql_insert_blob = conn.prepareStatement(sql_insert_chunks);
                sql_insert_blob.setBinaryStream(2, chunk, chunkSize);

                buffer.write(chunkByte,0,chunk1.read(chunkByte));

                sql_insert_blob.setString(1,sha256.getSha256(buffer.toString()));
                sql_insert_blob.executeUpdate();
                fileRecipe.write(sha256.getSha256(buffer.toString()));
                numberOfChunks -= 1;
            }

            if (lastChunkSize > 0){

                PreparedStatement sql_insert_blob = null;
                sql_insert_blob = conn.prepareStatement(sql_insert_chunks);
                sql_insert_blob.setBinaryStream(2, chunk, lastChunkSize);

                buffer.write(lastChunkByte,0,chunk1.read(lastChunkByte));

                sql_insert_blob.setString(1,sha256.getSha256(buffer.toString()));
                sql_insert_blob.executeUpdate();
                fileRecipe.write(sha256.getSha256(buffer.toString()));
            }
            fileRecipe.close();
        } catch (Exception e) {
            if (conn != null) {
                try {
                    stmt.close();
                    conn.rollback();
                    System.err.print("Transaction is being rolled back");
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            }


        } finally {
                if (stmt != null) {
                    try{
                        stmt.close();
                        conn.setAutoCommit(true);
                    } catch (SQLException se) {
                        se.printStackTrace();
                    }
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        System.out.println("OK!");
    }
}
