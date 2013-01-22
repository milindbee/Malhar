/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.algo.MatchStringMap;
import com.malhartech.lib.util.CombinerHashMap;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * A compare operation is done on String tuple based on the property "key", "value", and "cmp" both matching and non matching tuples on emitted on respective ports. If the tuple
 * passed the test, it is emitted on the output port "compare". If the tuple fails it is emitted on port "except". The comparison is done parsing a double
 * value from the String. Both output ports are optional, but at least one has to be connected<p>
 *  * This module is a pass through<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects Map&lt;K,String&gt;<br>
 * <b>compare</b>: emits HashMap&lt;K,String&gt;<br>
 * <b>except</b>: emits HashMap&lt;K,String&gt;<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key on which compare is done<br>
 * <b>value</b>: The value to compare with<br>
 * <b>cmp</b>: The compare function. Supported values are "lte", "lt", "eq", "neq", "gt", "gte". Default is "eq"<br>
 * <br>
 * <b>Compile time checks</b>:<br>
 * Key must be non empty<br>
 * Value must be able to convert to a "double"<br>
 * Compare string, if specified, must be one of "lte", "lt", "eq", "neq", "gt", "gte"<br>
 * <b>Specific run time checks</b>:<br>
 * Does the incoming HashMap have the key<br>
 * Is the value of the key a number<br>
 * <p>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for CompareExceptStringMap&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>5 Million K,String pairs/s</b></td><td>Each tuple is emitted if emitError is set to true</td><td>In-bound rate determines performance as every tuple is emitted.
 * Immutable tuples were used in the benchmarking. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=String); emitError=true; key=a; value="3.0"; cmp=eq</b>:
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for CompareExceptStringMap&lt;K,V extends Number&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th colspan=2>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(Map&lt;K,String&gt;)</th><th><i>compare</i>(HashMap&lt;K,String&gt;)</th><th><i>except</i>(HashMap&lt;K,String&gt;)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td></td><td>{a=2,b=20,c=1000}</td></tr>
 * <tr><td>Data (process())</td><td>{a=3,b=40,c=2}</td><td>{a=3,b=40,c=2}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td><td>{a=10,b=5}</td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td><td>{d=55,b=12}</td></tr>
 * <tr><td>Data (process())</td><td>{d=22,a=4}</td><td></td><td>{d=22,a=4}</td></tr>
 * <tr><td>Data (process())</td><td>{d=4,a=3,g=5,h=44}</td><td>{d=4,a=3,g=5,h=44}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */
public class CompareExceptStringMap<K> extends MatchStringMap<K,String>
{
  @OutputPortFieldAnnotation(name = "compare", optional=true)
  public final transient DefaultOutputPort<HashMap<K,String>> compare = match;

  @OutputPortFieldAnnotation(name = "except", optional=true)
  public final transient DefaultOutputPort<HashMap<K,String>> except = new DefaultOutputPort<HashMap<K,String>>(this)
  {
    @Override
    public Unifier<HashMap<K, String>> getUnifier()
    {
      return new CombinerHashMap<K, String>();
    }
  };

  /**
   * Emits if compare port is connected
   * @param tuple
   */
  @Override
  public void tupleMatched(Map<K,String> tuple)
  {
    if (compare.isConnected()) {
      compare.emit(cloneTuple(tuple));
    }
  }

  /**
   * Emits if except port is connected
   * @param tuple
   */
  @Override
  public void tupleNotMatched(Map<K,String> tuple)
  {
    if (except.isConnected()) {
      except.emit(cloneTuple(tuple));
    }
  }
}