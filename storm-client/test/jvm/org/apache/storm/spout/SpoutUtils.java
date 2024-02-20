package org.apache.storm.spout;

import org.apache.storm.task.TopologyContext;

import static org.mockito.Mockito.*;

public class SpoutUtils {
    public static TopologyContext topoCtxBuilder(int v){
        TopologyContext tc = mock(TopologyContext.class);
        switch (v){
            case 1: //valid
                when(tc.getThisComponentId()).thenReturn("test");
                when(tc.getThisTaskId()).thenReturn(1);
                return tc;
            case 2: //invalid
                doThrow(new RuntimeException("Invalid conf")).when(tc).getThisComponentId();
                doThrow(new RuntimeException("Invalid conf")).when(tc).getThisTaskId();
                doThrow(new RuntimeException("Invalid conf")).when(tc).getHooks();
                doThrow(new RuntimeException("Invalid conf")).when(tc).getThisTaskIndex();
                return tc;
            default:
                return null;

        }

    }
}
