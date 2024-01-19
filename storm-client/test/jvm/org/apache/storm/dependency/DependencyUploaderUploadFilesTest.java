package org.apache.storm.dependency;

import org.apache.storm.generated.AuthorizationException;
import org.junit.Assert;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
class DependencyUploaderUploadFilesTest {

    private List<File> dependencies;
    private boolean cleanupIfFails;
    private boolean expectedResult;

    enum FileObjEnum{
        VALID,
        INVALID,
        EMPTY
    }



    public static List<File> depecdenciesBuilder(int ob){
        List<File> ret = new ArrayList<>();
        switch (ob){

            case 1:
                File temp = null;
                try {
                    temp =  File.createTempFile("prefix-", "-suffix");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                ret.add(temp);
            case 2:
                ret.add(getMockedNotValidFile());
            case 3:
                //nothing to do here
            default:
                //nothing to do here
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


    public DependencyUploaderUploadFilesTest(List<File> dependencies, boolean cleanupIfFails, boolean expectedResult) {
        this.dependencies = dependencies;
        this.cleanupIfFails = cleanupIfFails;
        this.expectedResult = expectedResult;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() throws FileNotFoundException {
        // Creazione dell'oggetto ByteBuf con le stesse proprietà
        return Arrays.asList(new Object[][]{
                {depecdenciesBuilder(1), false, true},
        });
    }

    @Test
    public void testuploadFilesTest(){

        DependencyUploader du = new DependencyUploader();
        try {
            du.uploadFiles(this.dependencies, this.cleanupIfFails);
        } catch (IOException | AuthorizationException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(true);
    }





}