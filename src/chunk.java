import org.apache.commons.codec.digest.DigestUtils;


public class chunk {

    private int chunkSize = 512;
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

    public String generateChunkID(byte[] chunkByte){

        this.chunkID = DigestUtils.sha256Hex(chunkByte);
        return chunkID;
    }

    //Constructors
    public chunk() {

        this.chunkSize = chunkSize;
    }
}
