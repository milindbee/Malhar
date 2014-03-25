package com.datatorrent.contrib.r;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RScriptOperatorBooleanTest {
    RScript oper = new RScript("r/aBoolean.R","aBoolean", "retVal");

    @Test
    public void testString(){

        oper.setup(null);
        oper.beginWindow(0);

        CountAndLastTupleTestSink hashSink = new CountAndLastTupleTestSink();
        oper.boolOutput.setSink(hashSink);
        CountAndLastTupleTestSink hashSinkAP = new CountAndLastTupleTestSink();
        oper.boolArrayOutput.setSink(hashSinkAP);

        Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();
        argTypeMap.put("AREA", RScript.REXP_TYPE.REXP_INT);
        argTypeMap.put("DUMMY", RScript.REXP_TYPE.REXP_BOOL);
        argTypeMap.put("DUMMY_ARR", RScript.REXP_TYPE.REXP_ARRAY_BOOL);

        oper.setArgTypeMap(argTypeMap);

        HashMap map = new HashMap();

        boolean smallStates[] = new boolean[]{true, false, true, false};

        //Return the value passed for 'DUMMY'
        // A boolean value being passed and returned
        map.put("AREA", 10);
        map.put("DUMMY", true);
        map.put("DUMMY_ARR", smallStates);
        oper.inBindings.process(map);

        // Return a subset (array) of the value passed for 'DUMMY_ARR'
        // A boolean array value being passed and returned
        oper.setFunctionName("aBooleanArrayAccepted");
        map = new HashMap();
        map.put("AREA", 10000);
        map.put("DUMMY", false);
        map.put("DUMMY_ARR", smallStates);
        oper.inBindings.process(map);

        // Return a subset (array) of the value passed for 'DUMMY_ARR'
        // A boolean array value being returned
        oper.setFunctionName("aBooleanArrayReturned");
        map = new HashMap();
        map.put("AREA", 10000);
        map.put("DUMMY", false);
        map.put("DUMMY_ARR", smallStates);
        oper.inBindings.process(map);

        oper.endWindow();
        oper.teardown();

        Assert.assertEquals("Number of boolean values returned : ", 1, hashSink.count);
        System.out.println("Number of boolean values returned : " + hashSink.count);

        Assert.assertEquals("Number of boolean values returned : ", 2, hashSinkAP.count);
        System.out.println("Number of boolean arrays returned : " + hashSinkAP.count);
    }


}
