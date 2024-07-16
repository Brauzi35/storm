package org.apache.storm.bolt;

import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.apache.storm.windowing.TupleWindowImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class JoinBoltTest {

    private final TupleWindow input;
    private final Values expectedOutput;
    private final JoinBolt joinBolt;
    private final Boolean expectedResult;

    public JoinBoltTest(TupleWindow input, Values expectedOutput, JoinBolt joinBolt, Boolean expectedResult) {
        this.input = input;
        this.expectedOutput = expectedOutput;
        this.joinBolt = joinBolt;
        this.expectedResult = expectedResult;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {createValidTupleWindow(), new Values("value1"), createValidJoinBolt(), true},
                {createEmptyTupleWindow(), new Values(), createValidJoinBolt(), false},
                {createInvalidTupleWindow(), new Values(), createValidJoinBolt(), false},
                //miglioramenti
                {createDifferentStreamTupleWindow(), new Values(), createDifferentStreamJoinBolt(), true},
                {createValidTupleWindow(), new Values("value1"), createJoinBoltWithOutputStream(), true}
        });
    }

    @Test
    public void testJoinBolt() {
        MockCollector collector = new MockCollector();
        try {
            joinBolt.prepare(new HashMap<>(), null, collector);
            joinBolt.execute(input);

            List<List<Object>> emittedValues = collector.getEmittedValues();
            if (expectedOutput.isEmpty()) {
                System.out.println(emittedValues);
                assertEquals(0, emittedValues.size());
            } else {
                assertEquals(1, emittedValues.size());
                assertEquals(expectedOutput, new Values(emittedValues.get(0).toArray()));
            }
        } catch (RuntimeException re) {
            if (!expectedResult) {
                assertTrue(true);
            } else {
                assertTrue(false);
            }
        }
    }

    private static TupleWindow createValidTupleWindow() {
        List<String> fields = Collections.singletonList("field1");
        List<Object> values = Collections.singletonList("value1");
        Tuple tuple = new TupleImpl(new MockContext(fields.toArray(new String[0])), values, "source1", 0, "source1");
        List<Tuple> tuples = Collections.singletonList(tuple);
        return new TupleWindowImpl(tuples, null, null);
    }

    private static TupleWindow createInvalidTupleWindow() {
        return new TupleWindow() {
            @Override
            public List<Tuple> get() {
                throw new RuntimeException("Mocked exception for testing");
            }

            @Override
            public List<Tuple> getExpired() {
                return Collections.emptyList();
            }

            @Override
            public Long getEndTimestamp() {
                return null;
            }

            @Override
            public Long getStartTimestamp() {
                return null;
            }

            @Override
            public List<Tuple> getNew() {
                return Collections.emptyList();
            }
        };
    }

    private static TupleWindow createEmptyTupleWindow() {
        List<Tuple> tuples = Collections.emptyList();
        return new TupleWindowImpl(tuples, null, null);
    }

    private static TupleWindow createDifferentStreamTupleWindow() {
        List<String> fields = Collections.singletonList("field2");
        List<Object> values = Collections.singletonList("value2");
        Tuple tuple = new TupleImpl(new MockContext(fields.toArray(new String[0])), values, "source2", 0, "source2");
        List<Tuple> tuples = Collections.singletonList(tuple);
        return new TupleWindowImpl(tuples, null, null);
    }

    private static JoinBolt createValidJoinBolt() {
        JoinBolt joinBolt = new JoinBolt("source1", "field1");
        joinBolt.select("source1:field1");
        return joinBolt;
    }

    private static JoinBolt createDifferentStreamJoinBolt() {
        JoinBolt joinBolt = new JoinBolt("source1", "field1");
        joinBolt.join("source2", "field2", "source1");
        joinBolt.select("source1:field1, source2:field2");
        return joinBolt;
    }

    private static JoinBolt createJoinBoltWithOutputStream() {
        JoinBolt joinBolt = new JoinBolt("source1", "field1");
        joinBolt.withOutputStream("outputStream");
        joinBolt.select("source1:field1");
        return joinBolt;
    }

    static class MockCollector extends OutputCollector {
        private final List<List<Object>> emittedValues = new ArrayList<>();

        public MockCollector() {
            super(null);
        }

        @Override
        public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            emittedValues.add(tuple);
            return null;
        }

        @Override
        public List<Integer> emit(Collection<Tuple> anchors, List<Object> tuple) {
            emittedValues.add(tuple);
            return null;
        }

        public List<List<Object>> getEmittedValues() {
            return emittedValues;
        }
    }

    static class MockContext extends GeneralTopologyContext {
        private final Fields fields;

        public MockContext(String[] fieldNames) {
            super(null, new HashMap<>(), null, null, null, null);
            this.fields = new Fields(fieldNames);
        }

        @Override
        public String getComponentId(int taskId) {
            return "component";
        }

        @Override
        public Fields getComponentOutputFields(String componentId, String streamId) {
            return fields;
        }
    }
}
