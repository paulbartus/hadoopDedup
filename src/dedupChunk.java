import org.apache.commons.codec.digest.DigestUtils;


public class dedupChunk {

    private String chunkID;
    private byte[] chunkContent;
    private int chunkSize ;
    private boolean lastChunk;

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

    public boolean isLastChunk(){

        return lastChunk;
    }

    //Setters
    public void setChunkID(){

        this.chunkID = DigestUtils.sha256Hex(chunkContent);
    }

    public void setLastChunk() {
        this.lastChunk=true;
    }

    //Constructors
    public dedupChunk(int chunkSize, boolean lastChunk) {

        this.chunkSize = chunkSize;
        this.chunkContent = new byte[chunkSize];
        this.lastChunk = lastChunk;
    }
}
