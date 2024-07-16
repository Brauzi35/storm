package org.apache.storm.spout;

import org.apache.storm.state.InMemoryKeyValueState;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doThrow;

@RunWith(Parameterized.class)
public class CheckpointSpoutTest {

    //The Storm configuration for this spout.
    //This is the configuration provided to the topology merged in with cluster configuration on this machine
    private CheckpointSpout spout = new CheckpointSpout();
    private TopologyContext tc;
    private SpoutOutputCollector oc;
    private boolean expExc;

    public CheckpointSpoutTest(TopologyContext tc, SpoutOutputCollector oc, boolean expExc) {
        this.tc = tc;
        this.oc = oc;
        this.expExc = expExc;
    }
    @BeforeEach
    void setup(){
         this.spout = new CheckpointSpout();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() throws FileNotFoundException {
        // Creazione dell'oggetto ByteBuf con le stesse proprietà
        return Arrays.asList(new Object[][]{
               {topoCtxBuilder(1), getOutputCollector(1), false},
                {topoCtxBuilder(2), getOutputCollector(1), true},
                {topoCtxBuilder(1), getOutputCollector(2), true},
                {topoCtxBuilder(2), getOutputCollector(2), true},




        });
    }


    @Test
    public void openTest(){
        boolean res = true;
        try {

            this.spout.open(new HashMap<>(), this.tc, this.oc);

            this.spout.nextTuple();

        }catch (RuntimeException e) {
            System.out.println("catch");
            System.out.println(e.getMessage());
            if (!this.expExc){
                res = false;
            }
        }
        Assert.assertTrue(res);
    }

    @Test
    public void prepareTest(){

        KeyValueState<String, CheckPointState> state = new InMemoryKeyValueState<>();
        CheckPointState checkPointState = new CheckPointState(1L, CheckPointState.State.PREPARING);
        state.put("__state", checkPointState);
        boolean res = true;
        try{
            spout.open(this.tc, this.oc, 1000, state);
                }catch(RuntimeException e){
            System.out.println(e.getMessage());
                if(!this.expExc){

                    res = false;
                }
        }

        if(!this.expExc) {
            System.out.println("entrato if");


            AtomicBoolean check = new AtomicBoolean(false);
            doAnswer(invocationOnMock -> {
                check.set(true);
                return null;
            }).when(this.oc).emit(CheckpointSpout.CHECKPOINT_STREAM_ID, new Values(1L, CheckPointState.Action.ROLLBACK), 1L);

            spout.nextTuple();

            Assert.assertTrue(check.get());

            spout.ack(1L);

            AtomicBoolean check2 = new AtomicBoolean(false);
            doAnswer(invocationOnMock -> {
                check2.set(true);
                return null;
            }).when(this.oc).emit(CheckpointSpout.CHECKPOINT_STREAM_ID, new Values(0L, CheckPointState.Action.INITSTATE), 0L);

            spout.nextTuple();

            Assert.assertTrue(check2.get());

            spout.ack(0L);

            AtomicBoolean check3 = new AtomicBoolean(false);
            doAnswer(invocationOnMock -> {
                check3.set(true);
                return null;
            }).when(oc).emit(CheckpointSpout.CHECKPOINT_STREAM_ID, new Values(1L, CheckPointState.Action.PREPARE), 1L);

            spout.nextTuple();

            assertTrue(check3.get());

            spout.ack(1L);

            AtomicBoolean check4 = new AtomicBoolean(false);
            doAnswer(invocationOnMock -> {
                check4.set(true);
                return null;
            }).when(oc).emit(CheckpointSpout.CHECKPOINT_STREAM_ID, new Values(1L, CheckPointState.Action.COMMIT), 1L);

            spout.nextTuple();

            assertTrue(check4.get());



        }
        Assert.assertTrue(res); //res should always be true


    }

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

    public static SpoutOutputCollector getOutputCollector(int type) {
        SpoutOutputCollector outputCollector = mock(SpoutOutputCollector.class);
        switch (type) {
            case 1: //valid
                return outputCollector;
            case 2: //invalid
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(anyList());
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(anyList(), any());
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(anyString(), anyList(), any());
                doThrow(new RuntimeException("Invalid collector")).when(outputCollector).emit(anyString(), anyList());
                return outputCollector;
            default:
                fail("Unexpected type");
                return null;
        }
    }
}