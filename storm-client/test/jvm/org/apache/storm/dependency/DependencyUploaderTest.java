package org.apache.storm.dependency;

import org.apache.storm.blobstore.ClientBlobStore;
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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

//USA SEMPRE JUNIT4
@RunWith(Parameterized.class)
public class DependencyUploaderTest {


    private List<File> dependencies;
    private boolean cleanupIfFails;
    private boolean expectedResult;
    private DependencyUploader du;

    enum FileObjEnum{
        VALID,
        INVALID,
        EMPTY
    }



    public static List<File> depecdenciesBuilder(int ob){
        System.out.println("dependenciesbuilder");
        List<File> ret = new ArrayList<>();
        switch (ob){

            case 1:
                System.out.println("case 1");

                ret.add(getMockedValidFile());
                break;
            case 2:
                System.out.println("case 2");
                ret.add(getMockedNotValidFile());
                break;
            case 3:
                //nothing to do here
                System.out.println("case 3");
                break;
            default:
                //nothing to do here
                System.out.println("default");
                break;
        }

        return ret;
    }

    public static File getMockedNotValidFile(){
        File mockFile = mock(File.class);
        when(mockFile.getName()).thenReturn(null);
        when(mockFile.isFile()).thenReturn(false);
        when(mockFile.exists()).thenReturn(false);

        return mockFile;
    }

    private static File getMockedValidFile(){

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
        // Creazione dell'oggetto ByteBuf con le stesse proprietà
        return Arrays.asList(new Object[][]{
                {depecdenciesBuilder(1), false, true},
                {depecdenciesBuilder(2), false, false},
                {depecdenciesBuilder(3), false, true},
                {depecdenciesBuilder(1), true, true},
                {depecdenciesBuilder(2), true, false},
                {depecdenciesBuilder(3), true, true},



        });
    }
    @Before
    public void setup() throws AuthorizationException, KeyNotFoundException {
        this.du = new DependencyUploader();
        this.du.setBlobStore(getMockedCBS());
    }

    @After
    public void teardown(){
        this.du.shutdown();
    }

    private ClientBlobStore getMockedCBS() throws AuthorizationException, KeyNotFoundException {

        ClientBlobStore cbs = mock(ClientBlobStore.class);
        doNothing().when(cbs).deleteBlob(isA(String.class));
        when(cbs.getBlobMeta(isA(String.class))).thenReturn(new ReadableBlobMeta());
        return cbs;

    }

    @Test
    public void testuploadFilesTest(){

        boolean actual = true;

        try {
            du.uploadFiles(this.dependencies, this.cleanupIfFails);
        } catch (IOException | AuthorizationException | FileNotAvailableException e) {
            actual = false;
        }
        Assert.assertEquals(this.expectedResult, actual);
    }




}