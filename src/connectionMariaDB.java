
public class connectionMariaDB {

    private static String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static String DB_URL = "jdbc:mariadb://localhost/ext4";
    private static String DB_USER = "root";
    private static String DB_PASSWORD = "54h5dynKPf";

    //Getters
    public static String getJDBC_DRIVER() {

        return JDBC_DRIVER;
    }

    public static String getDB_URL() {

        return DB_URL;
    }

    public static String getDB_USER() {

        return DB_USER;
    }

    public static String getDB_PASSWORD() {

        return DB_PASSWORD;
    }

    //Setters
    public void setJDBC_DRIVER(String JDBC_DRIVER) {

        this.JDBC_DRIVER = JDBC_DRIVER;
    }

    public void setDB_URL(String DB_URL) {

        this.DB_URL = DB_URL;
    }

    public void setDB_USER(String DB_USER) {

        this.DB_USER = DB_USER;
    }

    public void setDB_PASSWORD(String DB_PASSWORD) {

        this.DB_PASSWORD = DB_PASSWORD;
    }

    //Constructors
    public connectionMariaDB(String JDBC_DRIVER, String DB_URL, String DB_USER, String DB_PASSWORD) {

        this.JDBC_DRIVER = JDBC_DRIVER;
        this.DB_URL = DB_URL;
        this.DB_USER = DB_USER;
        this.DB_PASSWORD = DB_PASSWORD;
    }

    public connectionMariaDB(){

    }
}
