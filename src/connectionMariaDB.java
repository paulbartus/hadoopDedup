
public class connectionMariaDB {

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL = "jdbc:mariadb://localhost/ext4";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "54h5dynKPf";

    //Getters
    public static String getJDBC_DRIVER() { return JDBC_DRIVER; }
    public static String getDB_URL() { return DB_URL; }
    public static String getDB_USER() { return DB_USER; }
    public static String getDB_PASSWORD() { return DB_PASSWORD; }

    //Constructors
    public connectionMariaDB(String JDBC_DRIVER, String DB_URL, String DB_USER, String DB_PASSWORD) {

    }

}
