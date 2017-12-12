import org.apache.commons.codec.digest.DigestUtils;


public class chunk {

    private String chunkID;
    private byte[] chunkContent;
    private int chunkSize ;

    //Getters
    public int getChunkSize(){

        return chunkSize;
    }

    public byte[] getChunkContent(){

        return chunkContent;
    }

    public String getChunkID(){

        return chunkID;
    }

    //Setters
    public void setChunkID(){

        this.chunkID = DigestUtils.sha256Hex(chunkContent);
    }

    //Constructors
    public chunk(int chunkSize) {

        this.chunkSize = chunkSize;
        this.chunkContent = new byte[chunkSize];
    }
}
