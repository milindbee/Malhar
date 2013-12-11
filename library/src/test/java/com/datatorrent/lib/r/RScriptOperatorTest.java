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
import com.datatorrent.lib.testbench.HashTestSink;
import com.datatorrent.lib.testbench.SumTestSink;
import junit.framework.Assert;
import org.junit.Test;
import org.rosuda.JRI.REXP;

import java.util.HashMap;
import java.util.Map;

public class RScriptOperatorTest {
    RScript oper = new RScript();

    @Test
    public void testRScript(){
        testInt();
        testReal();
        testString();
    }

    public void testInt(){

        oper.setup(null);
        oper.beginWindow(0);

        HashTestSink hashSink = new HashTestSink();
        oper.intOutput.setSink(hashSink);

        oper.setScript("/home/hduser/anInt.R");
        oper.setReturnVariable("retVal");

        Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();
        argTypeMap.put("num1", RScript.REXP_TYPE.REXP_INT);
        argTypeMap.put("num2", RScript.REXP_TYPE.REXP_INT);
        oper.setArgTypeMap(argTypeMap);

        HashMap map = new HashMap();

        map.put("num1", 5);
        map.put("num2", 12);
        oper.inBindings.process(map);
        map = new HashMap();

        map.put("num1", 102);
        map.put("num2", 12);
        oper.inBindings.process(map);

        oper.endWindow();

        Assert.assertEquals("Number of integer additions done : ",  2, hashSink.count);
        System.out.println("Number of integer additions done : " + hashSink.count);
    }

    public void testReal(){

        oper.beginWindow(0);

        HashTestSink hashSink = new HashTestSink();
        oper.doubleOutput.setSink(hashSink);

        oper.setScript("/home/hduser/aReal.R");
        oper.setReturnVariable("retVal");

        Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();
        argTypeMap.put("num1", RScript.REXP_TYPE.REXP_DOUBLE);
        argTypeMap.put("num2", RScript.REXP_TYPE.REXP_DOUBLE);
        oper.setArgTypeMap(argTypeMap);

        HashMap map = new HashMap();

        map.put("num1", 5.2);
        map.put("num2", 12.4);
        oper.inBindings.process(map);
        map = new HashMap();

        map.put("num1", 10.2);
        map.put("num2", 12.6);
        oper.inBindings.process(map);

        oper.endWindow();

        Assert.assertEquals("Number of real number additions done : ",  2, hashSink.count);
        System.out.println("Number of real number additions done : " + hashSink.count);
    }


    public void testString(){

        oper.beginWindow(0);

        HashTestSink hashSink = new HashTestSink();
        oper.strOutput.setSink(hashSink);

        oper.setScript("/home/hduser/aString.R");
        oper.setReturnVariable("retVal");

        Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();
        argTypeMap.put("str1", RScript.REXP_TYPE.REXP_STR);
        argTypeMap.put("str2", RScript.REXP_TYPE.REXP_STR);
        argTypeMap.put("seperator", RScript.REXP_TYPE.REXP_STR);
        oper.setArgTypeMap(argTypeMap);

        HashMap map = new HashMap();

        map.put("str1", "Hello");
        map.put("str2", "World");
        map.put("seperator", " ");

        oper.inBindings.process(map);
        map = new HashMap();

        map.put("str1", "Have a");
        map.put("str2", "great day !!!");
        map.put("seperator", "-");
        oper.inBindings.process(map);

        oper.endWindow();

        Assert.assertEquals("Number of strings returned after concatenation : ",  2, hashSink.count);
        System.out.println("Number of strings returned after concatenation : " + hashSink.count);
    }
}