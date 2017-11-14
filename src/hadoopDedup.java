
public class hadoopDedup {

    public static void main(String[] args) {

        file.dedupFile(file.getInputFileName());

        file.reconstructFile(file.getInputFileName());

    }
}

