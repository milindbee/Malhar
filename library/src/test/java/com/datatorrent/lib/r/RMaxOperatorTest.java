package com.datatorrent.lib.r;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import junit.framework.Assert;
import org.junit.Test;

public class RMaxOperatorTest {

    /**
     * Test oper logic emits correct results
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testNodeSchemaProcessing()
    {
        RMax<Double> oper = new RMax<Double>();
        CountAndLastTupleTestSink maxSink = new CountAndLastTupleTestSink();
        oper.max.setSink(maxSink);

        oper.setup(null);
        oper.beginWindow(0); //

        Double a = new Double(2.0);
        Double b = new Double(20.0);
        Double c = new Double(1000.0);

        oper.data.process(a);
        oper.data.process(b);
        oper.data.process(c);

        a = 1.0;
        oper.data.process(a);
        a = 10.0;
        oper.data.process(a);
        b = 5.0;
        oper.data.process(b);

        b = 12.0;
        oper.data.process(b);
        c = 22.0;
        oper.data.process(c);
        c = 14.0;
        oper.data.process(c);

        a = 46.0;
        oper.data.process(a);
        b = 2.0;
        oper.data.process(b);
        a = 23.0;
        oper.data.process(a);
        oper.endWindow(); //

        Assert.assertEquals("number emitted tuples", 1, maxSink.count);
        Assert.assertEquals("emitted high value was ", new Double(1000.0), maxSink.tuple);
        System.out.println(maxSink.count);
        System.out.println(maxSink.tuple);
    }
}