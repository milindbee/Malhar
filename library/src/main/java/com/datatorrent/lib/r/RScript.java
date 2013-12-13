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


import java.io.*;
import com.datatorrent.api.*;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.script.ScriptOperator;
import org.apache.commons.lang.mutable.MutableDouble;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This operator enables a user to execute a R script on tuples for Map<String, Object>.
 * The user should -
 * 1. set the name of the script to be executed,Make sure that the script file
 *    is available in teh classpath.
 * 2. set the name of the return variable
 * 3. set the type of arguments being passed. This will be done in a Map.
 * 4. Send the data in the form of a tuple consisting of a key:value pair where,
 *      "key" represents the name of the argument
 *      "value" represents the actual value of the argument.
 * A map of all the arguments is created and passed as input.
 * The result will be returned on one of the output ports depending on the type of the
 * return value.
 *
 * <b> Sample Usage Code : </b>
 * oper is an object of type RScript.
 *
 *  oper.setScriptFilePath("<script name>");
 *  oper.setReturnVariable("<name of the returned variable>");
 *
 *  Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>();
 *  argTypeMap.put(<argument name>, RScript.<argument type in the form of REXP_TYPE>);
 *  argTypeMap.put(<argument name>, RScript.<argument type in the form of REXP_TYPE>);
 *  ...
 *  ...
 *
 *  oper.setArgTypeMap(argTypeMap);
 *
 *  HashMap map = new HashMap();
 *
 *  map.put("<argument name>", <argument value>);
 *  map.put("<argument name>", <argument value>);
 *  ...
 *  ...
 *
 *  Note that the number of arguments inserted into the map should be same in number and order
 *  as that mentioned in the argument type map above it.
 *
 *  Pass this 'map' to the operator now.
 *
 * Currently, support has been added for only int, real and string type of values to be
 * passed and returned from th R scripts.
 *
 *
 * */
public class RScript extends ScriptOperator {

    public Map<String, REXP_TYPE> getArgTypeMap() {
        return argTypeMap;
    }

    public void setArgTypeMap(Map<String, REXP_TYPE> argTypeMap) {
        this.argTypeMap = argTypeMap;
    }

    public enum REXP_TYPE {
        REXP_INT(1), REXP_DOUBLE(2), REXP_STR(3), REXP_BOOL(6),
        REXP_ARRAY_INT(32), REXP_ARRAY_DOUBLE(33), REXP_ARRAY_STR(34);

        private int value;

        REXP_TYPE(int value) {
            this.value = value;
        }
    }

    @NotNull
    private Map<String, REXP_TYPE> argTypeMap;

    private List<Map<String, Object>> tupleList = new ArrayList<Map<String, Object>>();
    private String returnVariable;



    protected String scriptFilePath;

    private Rengine rengine;
    private static Logger log = LoggerFactory.getLogger(RScript.class);

    @Override
    public Map<String, Object> getBindings() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    // Gt the value of the name of the variable being returned
    public String getReturnVariable() {
        return returnVariable;
    }

    // Set teh name for the return variable
    public void setReturnVariable(String returnVariable) {
        this.returnVariable = returnVariable;
    }

    // Get the value of the script file with path as specified.
    public String getScriptFilePath() {
        return scriptFilePath;
    }

    // Set the value of the script file which should be executed.
    public void setScriptFilePath(String scriptFilePath) {
        this.scriptFilePath = scriptFilePath;
    }

    // Output port on which an int type of value is returned.
    @OutputPortFieldAnnotation(name = "intOutput")
    public final transient DefaultOutputPort<Integer> intOutput = new DefaultOutputPort<Integer>();

    // Output port on which an double type of value is returned.
    @OutputPortFieldAnnotation(name = "doubleOutput")
    public final transient DefaultOutputPort<Double> doubleOutput = new DefaultOutputPort<Double>();

    // Output port on which an string type of value is returned.
    @OutputPortFieldAnnotation(name = "strOutput")
    public final transient DefaultOutputPort<String> strOutput = new DefaultOutputPort<String>();


    /**
     * Adds the received tuple to the tupleList. This list of tuples will be processed one at a time
     * by the RScript to be invoked.
     */
    @Override
    public void process(Map<String, Object> tuple)
    {
        tupleList.add(tuple);
    }

    /**
     * Execute R code with variable value map.
     * Here,the RScript will be called for each of the tuples.The data will be emitted on an outputport
     * depending on its type.
     * It is assumed that the downstream operator knows the type of data being emitted by this operator
     * and will be receiving input tuples from the right output port of this operator.
     */
    @Override
    public void endWindow() {

        if (tupleList.size() == 0) return;

        //Get each argument and assign it to be available to the script.
        for (Map<String, Object> tuple: tupleList) {

            for (Map.Entry<String, Object> entry : tuple.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                switch (argTypeMap.get(key)) {
                    case REXP_INT:
                        int[] iArr = new int[1];
                        iArr[0] = (Integer)value;
                        rengine.assign(key, new REXP(REXP.XT_INT, iArr));
                        break;
                    case REXP_DOUBLE:
                        double[] dArr = new double[1];
                        dArr[0] = (Double)value;
                        rengine.assign(key, new REXP(REXP.XT_DOUBLE, dArr));
                        break;
                    case REXP_STR:
                        String[] sArr = new String[1];
                        sArr[0] = (String)value;
                        rengine.assign(key, new REXP(REXP.XT_STR, sArr));
                        break;
                    case REXP_BOOL:
                        rengine.assign(key, new REXP(REXP.XT_BOOL, value));
                        break;
                    case REXP_ARRAY_INT:
                        rengine.assign(key, new REXP(REXP.XT_ARRAY_INT, value));
                        break;
                    case REXP_ARRAY_DOUBLE:
                        rengine.assign(key, new REXP(REXP.XT_ARRAY_DOUBLE, value));
                        break;
                    case REXP_ARRAY_STR:
                        rengine.assign(key, new REXP(REXP.XT_ARRAY_STR, value));
                        break;
                }
            }

            // Call the R script specified.
            REXP result = rengine.eval(super.script);
            REXP retVal = rengine.eval(getReturnVariable());


             // Get the returned value and emit it on the appropriate output port depending
             // on its datatype.
             switch (retVal.rtype) {
                case REXP.INTSXP :
                    int iData = retVal.asInt();
                    intOutput.emit(iData);
                    break;

                 case REXP.REALSXP :
                    double dData = retVal.asDouble();
                    doubleOutput.emit(dData);
                    break;

                case REXP.STRSXP:
                    String sData = retVal.asString();
                    strOutput.emit(sData);
                    break;

                default:
                    //<TBD> : Throw an exception for a data type not found
                    break;
            }
        }

        //Clear the list of tuples.
        tupleList.clear();
        return;
    }

    /*
    * Initialize the R engine
    */
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);

        // new R-engine
        rengine=new Rengine(new String [] {"--vanilla"}, false, null);
        if (!rengine.waitForR())
        {
            log.debug(String.format( "\nCannot load R"));
            return;
        }
        super.setScript(readFileAsString());
    }

    /*
    * This function reads the script which is to be executed.
     */
    private String readFileAsString() {
        StringBuffer fileData = new StringBuffer(1000);

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(this.scriptFilePath)));

            char[] buf = new char[1024];
            int numRead = 0;

            while ((numRead = reader.read(buf)) != -1) {
                String readData = String.valueOf(buf, 0, numRead);
                fileData.append(readData);
                buf = new char[1024];
            }

            reader.close();
        } catch (IOException ex){
            log.debug(String.format( "\nError reading the R script"));
            ex.printStackTrace();
        }

        return fileData.toString();
    }

    /*
    * Stop the R engine
    */
    @Override
    public void teardown() {
        if (rengine != null){
            rengine.end();
        }
    }
}

