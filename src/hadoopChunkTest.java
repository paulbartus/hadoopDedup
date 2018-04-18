import org.junit.Test;

import static org.junit.Assert.*;

public class hadoopChunkTest {

    hadoopChunk hadoopChunk1 = new hadoopChunk("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
            "subdir0/subdir0/blk_1073741914");
    hadoopChunk hadoopChunk2 = new hadoopChunk("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
            "subdir0/subdir0/blk_1073741918");
    hadoopChunk hadoopChunk3 = new hadoopChunk("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
            "subdir0/subdir0/blk_1073741920");
    hadoopChunk hadoopChunk4 = new hadoopChunk("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
            "subdir0/subdir0/blk_1073741922");
    hadoopChunk hadoopChunk5 = new hadoopChunk("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
            "subdir0/subdir0/blk_1073741927");



    @Test
    public void getInputFileName() throws Exception {

        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741914", hadoopChunk1.getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741918", hadoopChunk2.getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741920", hadoopChunk3.getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741922", hadoopChunk4.getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741927", hadoopChunk5.getInputFileName());


        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741914", hadoopChunk1.reconstructHadoopChunk().getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741918", hadoopChunk2.reconstructHadoopChunk().getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741920", hadoopChunk3.reconstructHadoopChunk().getInputFileName());
        assertEquals("datanode/current/BP-1863467410-136.145.57.93-1512838700777/current/finalized/" +
                "subdir0/subdir0/blk_1073741927", hadoopChunk5.reconstructHadoopChunk().getInputFileName());
    }

    @Test
    public void getFileLength() throws Exception {

        assertEquals(1048576, hadoopChunk1.getFileLength());
        assertEquals(1048576, hadoopChunk2.getFileLength());
        assertEquals(1048576, hadoopChunk3.getFileLength());
        assertEquals(1048576, hadoopChunk4.getFileLength());
        assertEquals(1048576, hadoopChunk5.getFileLength());
    }

    @Test
    public void computeFileLength() throws Exception {
        assertEquals(1048576, hadoopChunk1.computeFileLength());
        assertEquals(1048576, hadoopChunk2.computeFileLength());
        assertEquals(1048576, hadoopChunk3.computeFileLength());
        assertEquals(1048576, hadoopChunk5.computeFileLength());
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
    public void getNumberOfChunks() throws Exception {

        assertEquals(2048, hadoopChunk1.getNumberOfChunks());
        assertEquals(2048, hadoopChunk2.getNumberOfChunks());
        assertEquals(2048, hadoopChunk3.getNumberOfChunks());
        assertEquals(2048, hadoopChunk4.getNumberOfChunks());
        assertEquals(2048, hadoopChunk5.getNumberOfChunks());
    }

    @Test
    public void checkIfExistsInDB() throws Exception {

        assertTrue(hadoopChunk1.checkIfExistsInDB());
        assertTrue(hadoopChunk2.checkIfExistsInDB());
        assertTrue(hadoopChunk3.checkIfExistsInDB());
        assertTrue(hadoopChunk4.checkIfExistsInDB());
        assertTrue(hadoopChunk5.checkIfExistsInDB());
    }

    @Test
    public void generateFileID() throws Exception {

        assertEquals( "dded04305a82864b920b091093683a23", hadoopChunk1.generateFileID());
        assertEquals( "91bb867b4380570d648b33595aee94a4", hadoopChunk2.generateFileID());
        assertEquals( "cd1d93c92219b57791f8fa6665bc7f0d", hadoopChunk3.generateFileID());
        assertEquals( "75c51cfa5af9a5fd4d586f366dc79139", hadoopChunk5.generateFileID());
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
