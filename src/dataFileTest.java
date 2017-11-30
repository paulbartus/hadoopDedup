import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class dataFileTest {

    dataFile dedupFile1 = new dataFile("data/file1.png");
    dataFile dedupFile2 = new dataFile("data/file8.mp4");
    dataFile dedupFile3 = new dataFile("data/file9.pdf");
    dataFile dedupFile4 = new dataFile("data/file9.txt");

    @Test
    public void getInputFileName() throws Exception {

        assertEquals("data/file1.png", dedupFile1.getInputFileName());
        assertEquals("data/file8.mp4", dedupFile2.getInputFileName());
        assertEquals("data/file9.pdf", dedupFile3.getInputFileName());
    }

    @Test
    public void getFileLength() throws Exception {

        long lengthFile1 = 679322;
        long lengthFile2 = 99069679;
        long lengthFile3 = 12517767;

        assertEquals(lengthFile1, dedupFile1.getFileLength());
        assertEquals(lengthFile2, dedupFile2.getFileLength());
        assertEquals(lengthFile3, dedupFile3.getFileLength());
    }

    @Test
    public void getFileParent() throws Exception {

        assertEquals("data", dedupFile1.getFileParent());
        assertNotEquals("/data/", dedupFile2.getFileParent());
        assertNotEquals("data/", dedupFile3.getFileParent());
    }

    @Test
    public void getLastChunkSize() throws Exception {

        assertTrue(dedupFile1.getLastChunkSize()<512);
        assertTrue(dedupFile2.getLastChunkSize()<512);
        assertTrue(dedupFile3.getLastChunkSize()<512);
        assertEquals(410, dedupFile1.getLastChunkSize());
        assertEquals(239, dedupFile2.getLastChunkSize());
        assertEquals(391, dedupFile3.getLastChunkSize());
    }

    @Test
    public void getNumberOfChunks() throws Exception {

        assertEquals(1327, dedupFile1.getNumberOfChunks());
        assertEquals(193496, dedupFile2.getNumberOfChunks());
        assertEquals(24449, dedupFile3.getNumberOfChunks());
    }

    @Test
    public void checkIfExistsInDB() throws Exception {

        assertTrue(dedupFile1.checkIfExistsInDB());
        assertTrue(dedupFile2.checkIfExistsInDB());
        assertTrue(dedupFile3.checkIfExistsInDB());
        assertFalse(dedupFile4.checkIfExistsInDB());
    }

    @Test
    public void generateFileID() throws Exception {

        assertEquals( "bfd9c1af13ec84176b2556ff76e3eb0d",
                dedupFile1.generateFileID(dedupFile1.getInputFileName()));

        assertEquals( "1707920ce89b69091fb1da02ec86d3e2",
                dedupFile2.generateFileID(dedupFile2.getInputFileName()));

        assertEquals( "139685fcf4cbdd0efa8761a96189f14b",
                dedupFile3.generateFileID(dedupFile3.getInputFileName()));
    }

    @Test
    public void insertIntoDB() throws Exception {
    }

    @Test
    public void dedupFile() throws Exception {
        dedupFile1.dedupFile();
        dedupFile2.dedupFile();
        dedupFile3.dedupFile();
    }

    @Test
    public void reconstructFile() throws Exception {

        File recFile1 = dedupFile1.reconstructFile();
        File recFile2 = dedupFile2.reconstructFile();
        File recFile3 = dedupFile3.reconstructFile();

        assertEquals(recFile1.length(),dedupFile1.getFileLength());
        assertEquals(recFile2.length(),dedupFile2.getFileLength());
        assertEquals(recFile3.length(),dedupFile3.getFileLength());

        assertEquals(dedupFile1.generateFileID(dedupFile1.getInputFileName()),
                dedupFile1.generateFileID(recFile1.getCanonicalPath()));

        assertEquals(dedupFile2.generateFileID(dedupFile2.getInputFileName()),
                dedupFile2.generateFileID(recFile2.getCanonicalPath()));

        assertEquals(dedupFile3.generateFileID(dedupFile3.getInputFileName()),
                dedupFile3.generateFileID(recFile3.getCanonicalPath()));
    }
}
