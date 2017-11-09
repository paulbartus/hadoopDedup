import java.sql.*;

public class connectMariaDB {
    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "jdbc:mariadb://localhost/ext4";
    static final String DB_USER = "root";
    static final String DB_PASSWORD = "54h5dynKPf";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        int chunkSize = 512;
        int fileSize = 555555555;
        int nchunks = fileSize / chunkSize;

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

            String sql_create_table_chunks = "CREATE TABLE IF NOT EXISTS chunks "
                    + "(id CHAR (64) PRIMARY KEY NOT NULL, "
                    + "count INT, "
                    + "content BLOB ) ENGINE = ARIA";

            String sql_check_if_exists_file = "SELECT EXISTS (SELECT id FROM files WHERE "
                    + "name = 'data1.csv')";

            String sql_insert_file = "INSERT IGNORE INTO files(id, name, size, chunksize) VALUES ('"
                    + md5.getMD5("file content")
                    + "', "
                    + "'data112.csv'"
                    + ", "
                    + fileSize
                    + ", "
                    + chunkSize
                    + ")";

            String sql_insert_chunks = "INSERT IGNORE INTO chunks(id, count, content) VALUES('"
                    + sha256.getSha256("chunk content")
                    + "', "
                    + 1
                    + ", "
                    + "'testtestets'"
                    + ") ON DUPLICATE KEY UPDATE count=count+1";

            stmt.executeUpdate(sql_create_table_files);
            stmt.executeUpdate(sql_create_table_chunks);
            System.out.println(" Successfully.");

            System.out.print("Checking if the file exists in the given database... ");

            ResultSet fileExists = stmt.executeQuery(sql_check_if_exists_file);
            fileExists.next();
            boolean exists = fileExists.getBoolean(1);
            if (exists) {
                System.out.println("The file already exists!");
            }
            else {
                System.out.println("The file does not exists!");
            }

            stmt.executeQuery(sql_insert_file);
            stmt.executeQuery(sql_insert_chunks);

            System.out.println("Done.");

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
