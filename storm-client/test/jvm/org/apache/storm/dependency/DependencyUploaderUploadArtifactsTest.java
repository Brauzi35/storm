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
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class DependencyUploaderUploadArtifactsTest {

    private Map<String, File> artifacts;
    private boolean expRes;
    private DependencyUploader du;


    @Before
    public void setup() throws AuthorizationException, KeyNotFoundException {
        this.du = new DependencyUploader();
        this.du.setBlobStore(getMockedCBS());
    }

    @After
    public void teardown(){
        this.du.shutdown();
    }

    public DependencyUploaderUploadArtifactsTest(Map<String, File> artifacts, boolean expRes) {
        this.artifacts = artifacts;
        this.expRes = expRes;
    }

    private ClientBlobStore getMockedCBS() throws AuthorizationException, KeyNotFoundException {

        ClientBlobStore cbs = mock(ClientBlobStore.class);
        doNothing().when(cbs).deleteBlob(isA(String.class));
        when(cbs.getBlobMeta(isA(String.class))).thenReturn(new ReadableBlobMeta());
        return cbs;

    }

    private static Map<String, File> artifactsBuilder(int ob){
        Map<String, File> ret = new HashMap<>();
        switch (ob) {
            case 1: //valid

                System.out.println("fino a qui ok");
                ret.put("valid", getMockedValidFile());
                System.out.println("fino a qui pure");
                break;
            case 2: //invalid
                System.out.println("fino a qui ok");
                ret.put("invalid", getMockedNotValidFile());
                break;
            case 3: //empty
                break;
            default:
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

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters(){
        return Arrays.asList(new Object[][]{
                {artifactsBuilder(1), true},
                {artifactsBuilder(2), false},
                {artifactsBuilder(3), true},


        });
    }

    @Test
    public void test(){


        boolean actual = true;
        List<String> ret = null;

        try {
            ret = this.du.uploadArtifacts(this.artifacts);

        }catch (FileNotAvailableException e){
            if(this.expRes){
                actual = false;
            }
        }

        if(this.expRes){
            System.out.println(ret);
        }

        Assert.assertTrue(actual);
    }




}