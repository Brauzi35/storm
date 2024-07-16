package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.*;

// USA SEMPRE JUNIT4
@RunWith(Parameterized.class)
public class DependencyUploaderTest {

    private List<File> dependencies;
    private boolean cleanupIfFails;
    private boolean expectedResult;
    private DependencyUploader du;
    private ClientBlobStore mockBlobStore;

    enum FileObjEnum {
        VALID,
        INVALID,
        EMPTY
    }

    public static List<File> dependenciesBuilder(int ob) {
        System.out.println("dependenciesbuilder");
        List<File> ret = new ArrayList<>();
        switch (ob) {
            case 1:
                System.out.println("case 1");
                ret.add(getMockedValidFile());
                break;
            case 2:
                System.out.println("case 2");
                ret.add(getMockedNotValidFile());
                break;
            case 3:
                // nothing to do here
                System.out.println("case 3");
                break;
            case 4:
                // miglioramento
                ret.add(getMockedThrowingFile());
                break;
            default:
                // nothing to do here
                System.out.println("default");
                break;
        }

        return ret;
    }

    public static File getMockedNotValidFile() {
        File mockFile = mock(File.class);
        when(mockFile.getName()).thenReturn(null);
        when(mockFile.isFile()).thenReturn(false);
        when(mockFile.exists()).thenReturn(false);
        return mockFile;
    }

    private static File getMockedThrowingFile() {
        File mockFile = mock(File.class);
        when(mockFile.getName()).thenReturn("throwingFile");
        return mockFile;
    }

    private static File getMockedValidFile() {
        File mockFile = mock(File.class);
        when(mockFile.getName()).thenReturn("validFile");
        when(mockFile.isFile()).thenReturn(true);
        when(mockFile.exists()).thenReturn(true);
        return mockFile;
    }

    public DependencyUploaderTest(List<File> dependencies, boolean cleanupIfFails, boolean expectedResult) {
        this.dependencies = dependencies;
        this.cleanupIfFails = cleanupIfFails;
        this.expectedResult = expectedResult;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() throws FileNotFoundException {
        return Arrays.asList(new Object[][] {
                {dependenciesBuilder(1), false, true},
                {dependenciesBuilder(2), false, false},
                {dependenciesBuilder(3), false, true},
                {dependenciesBuilder(1), true, true},
                {dependenciesBuilder(2), true, false},
                {dependenciesBuilder(3), true, true},
                {dependenciesBuilder(4), false, false},
                {dependenciesBuilder(4), true, false},
        });
    }

    @Before
    public void setup() throws AuthorizationException, KeyNotFoundException {
        this.du = new DependencyUploader();
        this.mockBlobStore = getMockedCBS();
        this.du.setBlobStore(mockBlobStore);
    }

    @After
    public void teardown() {
        this.du.shutdown();
    }

    private ClientBlobStore getMockedCBS() throws AuthorizationException, KeyNotFoundException {
        ClientBlobStore cbs = mock(ClientBlobStore.class);
        doNothing().when(cbs).deleteBlob(isA(String.class));
        when(cbs.getBlobMeta(isA(String.class))).thenReturn(new ReadableBlobMeta());
        return cbs;
    }

    @Test
    public void testuploadFilesTest() {
        //setup
        if (dependencies.stream().anyMatch(file -> "throwingFile".equals(file.getName()))) {
            try {
                doThrow(new RuntimeException("Mocked exception for testing"))
                        .when(mockBlobStore)
                        .createBlob(anyString(), isA(SettableBlobMeta.class));
            } catch (Exception e) {
                // Handle setup exception
            }
        }

        boolean actual = true;
        try {
            List<String> ret = du.uploadFiles(this.dependencies, this.cleanupIfFails);
            //voglio controllare che il valore di ritorno sia quello atteso
            if(!dependencies.isEmpty()){
                Assert.assertTrue(ret.get(0).contains("validFile"));
            }else{
                //se invece le dependencies erano lista vuota, anche ret deve essere vuota
                Assert.assertTrue(ret.isEmpty());
            }
        } catch (IOException | AuthorizationException | FileNotAvailableException e) {
            actual = false;
        } catch (RuntimeException e) {
            if ("Mocked exception for testing".equals(e.getMessage())) {
                actual = false;
            } else {
                throw e;
            }
        }

        Assert.assertEquals(this.expectedResult, actual);
    }
}
