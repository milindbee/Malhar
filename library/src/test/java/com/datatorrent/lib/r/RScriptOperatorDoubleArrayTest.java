/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.r;

import com.datatorrent.lib.testbench.CountAndLastTupleTestSink;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RScriptOperatorDoubleArrayTest {
    RScript oper = new RScript("r/aDoubleVector.R", "DV", "retVal");

    @Test
    public void testDoubleArray(){

//        oper.setScriptFilePath("r/aDoubleVector.R");
//        oper.setFunctionName("DV");
//        oper.setReturnVariable("retVal");
//        oper.setRuntimeFileCopy(true);

        oper.setup(null);
        oper.beginWindow(0);

        CountAndLastTupleTestSink hashSink = new CountAndLastTupleTestSink();
        oper.doubleArrayOutput.setSink(hashSink);

        Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();

        argTypeMap.put("num1", RScript.REXP_TYPE.REXP_ARRAY_DOUBLE);
        argTypeMap.put("num2", RScript.REXP_TYPE.REXP_ARRAY_DOUBLE);

        oper.setArgTypeMap(argTypeMap);

        HashMap map = new HashMap();

        double dArr[] = new double[5];
        dArr[0] = 0.0;
        dArr[1] = 1.1;
        dArr[2] = 2.2;
        dArr[3] = 3.3;
        dArr[4] = 4.4;

        map.put("num1", dArr);

        double dArr1[] = new double[5];
        dArr1[0] = 5.5;
        dArr1[1] = 6.6;
        dArr1[2] = 7.7;
        dArr1[3] = 8.8;
        dArr1[4] = 9.9;

        map.put("num2", dArr1);


        oper.inBindings.process(map);
        oper.endWindow();
        oper.teardown();

        Assert.assertEquals("Number of real number additions done : ", 1, hashSink.count);
        System.out.println("Number of real number additions done : " + hashSink.count);
    }
}
