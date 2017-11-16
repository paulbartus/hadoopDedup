
public class chunk {

    private static int chunkSize = 512;

    //Getters
    public static int getChunkSize(){

        return chunkSize;
    }

    // Setters
    public void setChunkSize(int chunkSize){

        this.chunkSize = chunkSize;
    }

    //Constructors
    public chunk(int chunkSize) {

        this.chunkSize = chunkSize;
    }
}
