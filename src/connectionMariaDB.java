import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class connectionMariaDB {

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL = "jdbc:mariadb://localhost:3306/ext4";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "54h5dynKPf";

    public Connection getDBConnection() throws Exception {

        try {

            Class.forName(JDBC_DRIVER);

        } catch (ClassNotFoundException exception) {

            System.out.println(exception.getMessage());
        }

        try {

            Connection connectMariaDB = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            return connectMariaDB;

        } catch (SQLException exception) {

            System.out.println(exception.getMessage());
        }
        return null;
    }

    public void closeDBConnection(Connection connectMariaDB) throws Exception {

        connectMariaDB.close();

    }
}