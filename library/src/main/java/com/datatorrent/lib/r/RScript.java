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
import org.rosuda.REngine.JRI.JRIEngine;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REngine;
import org.rosuda.REngine.REngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This operator enables a user to execute a R script on tuples for Map<String, Object>.
 * The script should be in the form of a function. This function will then be called by the operator.
 * The operator sources the file which contains the function (i.e. the script) during the starting
 * phase. That enables it to have the function loaded in memory. This function is then called by the
 * operator.
 * The user should -
 * 1. set the nam eof the script file (which contains the script in the form of a function)
 * 2. set the function name.
 * 3. set the name of the return variable
 * 4. set the name of the script to be executed.
 * 5. Set whether the file should be copied on the node before executing i.e. if the file
 *    has been manually copied on the node in the cluster, this operator does not have to
 *    copy. If not, the operator should do so. The user needs to tell the operator either ways.
 *    By default, the operator assumes that the file is copied on the node in the right location.
 *    If the operator is to copy hte file, make sure that the script file is available in the classpath.
 * 6. set the type of arguments being passed. This will be done in a Map.
 * 7. Send the data in the form of a tuple consisting of a key:value pair where,
 *      "key" represents the name of the argument
 *      "value" represents the actual value of the argument.
 * A map of all the arguments is created and passed as input.
 * The result will be returned on one of the output ports depending on the type of the
 * return value.
 *
 * <b> Sample Usage Code : </b>
 *  oper is an object of type RScript.
 *
 *  oper.setScriptFilePath("<script name>");
 *  oper.setFunctionName("<name of the function which has been given to the script inside he script file");
 *  oper.setReturnVariable("<name of the returned variable>");
 *  oper.setRuntimeFileCopy(false);
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
 *  Currently, support has been added for only int, real and string type of values and the corresonding arrays
 *  to be passed and returned from the R scripts.
 *
 *
 *
 * */


 public class RScript extends ScriptOperator {

    private static final long serialVersionUID = 201401161205L;

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

    // Lists to collect the tuples to be emitted on teh output port(s)
    private List<Integer> intList = new ArrayList<Integer>();
    private List<Double> doubleList = new ArrayList<Double>();
    private List<String> strList =new ArrayList<String>();

    private List<Integer[]> intArrayList = new ArrayList<Integer[]>();
    private List<Double[]> doubleArrayList = new ArrayList<Double[]>();
    private List<String[]> strArrayList = new ArrayList<String[]>();

    // Name of the file to be created at runtime if 'runtimeFileCOpy' is set to 'true'.
    transient private String tmpFileName;

    // Nam eof the return variable
    private String returnVariable;

    // Function name given to the script inside the script file.
    private String functionName;

    // Is the script file already available on the node where it will be executed.
    // If this this is set to 'true' it means that the script file is not available
    // and should be copied onto the node at runtime.
    //
    // The default value is 'false' meaning the sys administrator has ensured that
    // the script file is available on the node where this operator is running.
    // Hence the operator does not have to copy the file at runtime.
    //
    private boolean runtimeFileCopy = false;
    protected String scriptFilePath;

    private transient Rengine rengine;
    private static Logger log = LoggerFactory.getLogger(RScript.class);

    @Override
    public Map<String, Object> getBindings() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    // Get the value of the name of the variable being returned
    public String getReturnVariable() {
        return returnVariable;
    }

    // Set the name for the return variable
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

    public boolean isRuntimeFileCopy() {
        return runtimeFileCopy;
    }

    public void setRuntimeFileCopy(boolean runtimeFileCopy) {
        this.runtimeFileCopy = runtimeFileCopy;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
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
    //public final transient DefaultOutputPort<Map<String, Object>> strOutput = new DefaultOutputPort<Map<String, Object>>();


    // Output port on which an array of type int is returned.
    @OutputPortFieldAnnotation(name = "intArrayOutput")
    public final transient DefaultOutputPort<Integer[]> intArrayOutput = new DefaultOutputPort<Integer[]>();

    // Output port on which an array of type double is returned.
    @OutputPortFieldAnnotation(name = "doubleArrayOutput")
    public final transient DefaultOutputPort<Double[]> doubleArrayOutput = new DefaultOutputPort<Double[]>();

    // Output port on which an array of type str is returned.
    @OutputPortFieldAnnotation(name = "strArrayOutput")
    public final transient DefaultOutputPort<String[]> strArrayOutput = new DefaultOutputPort<String[]>();

    /**
     * Process the tuples
     */
    @Override
    public void process(Map<String, Object> tuple)
    {
        processTuple(tuple);
    }

    /*
    * Initialize the R engine, set the name of the script file to be executed.
    * If the script file to be executed on each node is to be copied by this operator, do so.
    */
    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);

        // new R-engine
        rengine=new Rengine(new String [] {"--vanilla"}, false, null);
        if (!rengine.waitForR())
        {
            log.debug(String.format( "\nCannot load R"));
            throw new RuntimeException("Cannot load R");
        }

        super.setScript(readFileAsString());

        if(this.isRuntimeFileCopy()){

            getFileName();
            writeStringAsFile(super.script, this.tmpFileName);
        }

        //Source the script file so as to load the function in memory
        if(isRuntimeFileCopy()){
            REXP result = rengine.eval("source(" + "\"" + this.tmpFileName + "\")");
        } else {
            REXP result = rengine.eval("source(\"" + getScriptFilePath() + "\")");
        }
    }


    /*
     * Emit the tuples from all the lists on the respective output ports
    */

    @Override
    public void endWindow() {

        //Emit and then clear the list of tuples.
        for (int i=0; i<intList.size(); i++) {
            intOutput.emit(intList.get(i));
        }
        intList.clear();

        for (int i=0; i<doubleList.size(); i++) {
            doubleOutput.emit(doubleList.get(i));
        }
        doubleList.clear();

        for (int i=0; i<intArrayList.size(); i++) {
            intArrayOutput.emit(intArrayList.get(i));
        }
        intArrayList.clear();

        for (int i=0; i<doubleArrayList.size(); i++) {
            doubleArrayOutput.emit(doubleArrayList.get(i));
        }
        doubleArrayList.clear();

        for (int i=0; i<strArrayList.size(); i++) {
            strArrayOutput.emit(strArrayList.get(i));
        }
        strArrayList.clear();


        for (int i=0; i<strList.size(); i++) {
            strOutput.emit(strList.get(i));
        }
        strList.clear();

        return;
    }



    /*
    * Stop the R engine and delete the script file if it was copied by this operator during the initial setup.
    */
    @Override
    public void teardown() {

        if(this.isRuntimeFileCopy()){

            File file = new File(this.tmpFileName);
            if (!file.delete()) {
                throw new RuntimeException("Error deleting file : " + this.tmpFileName);
            }
        }


        if (rengine != null){
            rengine.end();
        }
    }


    /*
    * Create a file name for the file to be created in the temporary directory. This name will consist of the
    * java tmp dir. + actual file name appended by the current time and thread id.
    * This is so as to make it unique and avoid overwriting any file with the same name already existing.
    * This will be needed when creating the file and will be used when sourcing the R script. The file
    * will be deleted later.
    */
    private void getFileName(){
        String[] data = getScriptFilePath().split(File.separator);
        String fileName = data[data.length - 1];
        this.tmpFileName = System.getProperty("java.io.tmpdir") + File.separatorChar + fileName + System.currentTimeMillis() + "_" + Thread.currentThread().getId();
        return;
    }

    /*
    * This function reads the script file - the R script file here, which is to be executed
    * and loads it into the memory.
    */
    private String readFileAsString() {
        StringBuffer fileData = new StringBuffer(1000);

        try {

            BufferedReader reader;

            if(isRuntimeFileCopy()){
                reader = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(this.scriptFilePath)));
            } else {
                reader = new BufferedReader(new FileReader(this.getScriptFilePath()));
            }


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
    * Name : writeStringAsFile() : Writes contents from memory into a file.
    * Arguments : contents from teh memory - to be written to the file
    *
    * Here, this function is used to create an R script file on a node before it can be sourced.
    * The file will be created in the default temporary-file directory of java. Once the file is sourced,
    * it will be deleted.
    */

    private void writeStringAsFile(String fileContent, String rFileName) {
        FileWriter fileWriter = null;
        try {
            String content = fileContent;
            File newRFile = new File(rFileName);
            fileWriter = new FileWriter(newRFile);
            BufferedWriter bw = new BufferedWriter(fileWriter);

            bw.write(content);
            bw.close();
        } catch (IOException ex) {
            log.error(String.format("\nError creating the R script file"));
        } finally {
            try {
                fileWriter.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }


    /**
     * Execute R code with variable value map.
     * Here,the RScript will be called for each of the tuples.The data will be emitted on an outputport
     * depending on its type.
     * It is assumed that the downstream operator knows the type of data being emitted by this operator
     * and will be receiving input tuples from the right output port of this operator.
     */

    public void processTuple(Map<String, Object> tuple){

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
                    int[] bArr = new int[1];
                    bArr[0] = (Integer)value;
                    rengine.assign(key, new REXP(REXP.XT_BOOL, bArr));
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
                default:
                    //<TBD> Log error
                    break;
            }
        }

        REXP result = rengine.eval("retVal<-" + getFunctionName() + "()");
        REXP retVal = rengine.eval(getReturnVariable());

        // Get the returned value and emit it on the appropriate output port depending
        // on its datatype.
        int len = 0;
        switch (retVal.rtype) {
            case REXP.INTSXP :
                len = retVal.asIntArray().length;
                int iData;

                if (len > 1){

                    Integer[] iAList = new Integer[len];
                    for (int i=0; i<len; i++) {
                        iAList[i] = (retVal.asIntArray()[i]);
                    }
                    intArrayList.add(iAList);
                }else {
                    iData = retVal.asInt();
                    intList.add(iData);
                }

                break;

            case REXP.REALSXP :
                len = retVal.asDoubleArray().length;
                double dData;

                if (len > 1){

                    Double[] dAList = new Double[len];
                    for (int i=0; i<len; i++) {
                        dAList[i] = (retVal.asDoubleArray()[i]);
                    }
                    doubleArrayList.add(dAList);
                }else {
                    dData = retVal.asDouble();
                    doubleList.add(dData);
                }

                break;

            case REXP.STRSXP:
                len = retVal.asStringArray().length;
                String sData;

                if (len > 1){

                    String[] sAList = new String[len];
                    for (int i=0; i<len; i++) {
                        sAList[i] = (retVal.asStringArray()[i]);
                    }
                    strArrayList.add(sAList);
                }else {
                    sData = retVal.asString();
                    strList.add(sData);
                }

                break;

            default:
                throw new IllegalArgumentException("Unsupported data type (" + retVal.rtype + ") returned ... ");
        }
    }

}

