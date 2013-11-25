package com.datatorrent.lib.r;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;

public class RScriptOperatorTest {
    /**
     * Test oper logic emits correct results
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testNodeSchemaProcessing()
    {
        RScript oper = new RScript();
        HashMap map = new HashMap();

        oper.setScript("/home/hduser/anInt.R");

        oper.setup(null);
        oper.beginWindow(0); //

        oper.inBindings.process(map);

        oper.setScript("/home/hduser/aReal.R");
        oper.inBindings.process(map);

        oper.setScript("/home/hduser/aString.R");
        oper.inBindings.process(map);

        oper.endWindow();

    }
}

