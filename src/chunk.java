
public class chunk {

    private int chunkSize;
    private String chunkID;

    //Getters
    public int getChunkSize(){

        return chunkSize;
    }

    public String getChunkID(){

        return chunkID;
    }

    // Setters
    public void setChunkSize(int chunkSize){

        this.chunkSize = chunkSize;
    }

    public void setChunkID(String chunkID){

        this.chunkID = chunkID;
    }

    //Constructors
    public chunk(int chunkSize) {

        this.chunkSize = chunkSize;
    }
}
