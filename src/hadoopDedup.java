
public class hadoopDedup {

    public static void main(String[] args) throws Exception {

        dataFile dedupFile = new dataFile("data/file2.png");

        dedupFile.dedupFile();

        dedupFile.reconstructFile();

    }
}

