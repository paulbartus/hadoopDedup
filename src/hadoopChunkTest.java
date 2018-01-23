import org.junit.Test;

import static org.junit.Assert.*;

public class hadoopChunkTest {

    hadoopChunk hadoopChunk1 = new hadoopChunk("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
            "subdir0/subdir0/blk_1073741987");
    hadoopChunk hadoopChunk2 = new hadoopChunk("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
            "subdir0/subdir0/blk_1073741988");
    hadoopChunk hadoopChunk3 = new hadoopChunk("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
            "subdir0/subdir0/blk_1073741989");
    hadoopChunk hadoopChunk4 = new hadoopChunk("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
            "subdir0/subdir0/blk_1073741990");
    hadoopChunk hadoopChunk5 = new hadoopChunk("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
            "subdir0/subdir0/blk_1073741825");



    @Test
    public void getInputFileName() throws Exception {

        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741987", hadoopChunk1.getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741988", hadoopChunk2.getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741989", hadoopChunk3.getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741990", hadoopChunk4.getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741825", hadoopChunk5.getInputFileName());


        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741987", hadoopChunk1.reconstructHadoopChunk().getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741988", hadoopChunk2.reconstructHadoopChunk().getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741989", hadoopChunk3.reconstructHadoopChunk().getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741825", hadoopChunk5.reconstructHadoopChunk().getInputFileName());
    }

    @Test
    public void getFileLength() throws Exception {

        assertEquals(1048576, hadoopChunk1.getFileLength());
        assertEquals(1048576, hadoopChunk2.getFileLength());
        assertEquals(1048576, hadoopChunk3.getFileLength());
        assertEquals(1048576, hadoopChunk4.getFileLength());
        assertEquals(679322, hadoopChunk5.getFileLength());
    }

    @Test
    public void getFileParent() throws Exception {

        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/subdir0/subdir0",
                hadoopChunk1.getFileParent());
        assertNotEquals("/datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/subdir0/subdir0/",
                hadoopChunk1.getFileParent());
        assertNotEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/subdir0/subdir0/",
                hadoopChunk1.getFileParent());
    }

    @Test
    public void getLastChunkSize() throws Exception {

        assertTrue(hadoopChunk1.getLastChunkSize()<512);
        assertTrue(hadoopChunk2.getLastChunkSize()<512);
        assertTrue(hadoopChunk3.getLastChunkSize()<512);
        assertTrue(hadoopChunk4.getLastChunkSize()<512);
        assertTrue(hadoopChunk5.getLastChunkSize()<512);
        assertEquals(0, hadoopChunk1.getLastChunkSize());
        assertEquals(0, hadoopChunk2.getLastChunkSize());
        assertEquals(0, hadoopChunk3.getLastChunkSize());
        assertEquals(410, hadoopChunk5.getLastChunkSize());
    }

    @Test
    public void getNumberOfChunks() throws Exception {

        assertEquals(2048, hadoopChunk1.getNumberOfChunks());
        assertEquals(2048, hadoopChunk2.getNumberOfChunks());
        assertEquals(2048, hadoopChunk3.getNumberOfChunks());
        assertEquals(2048, hadoopChunk4.getNumberOfChunks());
        assertEquals(1327, hadoopChunk5.getNumberOfChunks());
    }

    @Test
    public void checkIfExistsInDB() throws Exception {

        assertTrue(hadoopChunk1.checkIfExistsInDB());
        assertTrue(hadoopChunk2.checkIfExistsInDB());
        assertTrue(hadoopChunk3.checkIfExistsInDB());
        assertFalse(hadoopChunk4.checkIfExistsInDB());
        assertTrue(hadoopChunk5.checkIfExistsInDB());
    }

    @Test
    public void generateFileID() throws Exception {

        assertEquals( "7af7d2a632208c4a88aa7ba70cfe2d2e", hadoopChunk1.generateFileID());
        assertEquals( "a3d6064df7c3213374af6ae1938bdb66", hadoopChunk2.generateFileID());
        assertEquals( "f3d45e7f77758768afd93f15f140ed8d", hadoopChunk3.generateFileID());
        assertEquals( "bfd9c1af13ec84176b2556ff76e3eb0d", hadoopChunk5.generateFileID());
    }

    @Test
    public void insertIntoDB() throws Exception {
    }

    @Test
    public void dedupFile() throws Exception {
        hadoopChunk1.dedupHadoopChunk();
        hadoopChunk2.dedupHadoopChunk();
        hadoopChunk3.dedupHadoopChunk();
        hadoopChunk5.dedupHadoopChunk();
    }

    @Test
    public void reconstructFile() throws Exception {

        hadoopChunk reconstructedFile1 = hadoopChunk1.reconstructHadoopChunk();
        hadoopChunk reconstructedFile2 = hadoopChunk2.reconstructHadoopChunk();
        hadoopChunk reconstructedFile3 = hadoopChunk3.reconstructHadoopChunk();
        hadoopChunk reconstructedFile5 = hadoopChunk5.reconstructHadoopChunk();

        assertEquals(reconstructedFile1.getFileLength(), hadoopChunk1.getFileLength());
        assertEquals(reconstructedFile2.getFileLength(), hadoopChunk2.getFileLength());
        assertEquals(reconstructedFile3.getFileLength(), hadoopChunk3.getFileLength());
        assertEquals(reconstructedFile5.getFileLength(), hadoopChunk5.getFileLength());

        assertEquals(reconstructedFile1.generateFileID(), hadoopChunk1.generateFileID());
        assertEquals(reconstructedFile2.generateFileID(), hadoopChunk2.generateFileID());
        assertEquals(reconstructedFile3.generateFileID(), hadoopChunk3.generateFileID());
        assertEquals(reconstructedFile5.generateFileID(), hadoopChunk5.generateFileID());



    }
}
