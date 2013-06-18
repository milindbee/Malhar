/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC output adapter operator, for HashMap column mapping and transaction type database write. <p><br>
 * Each tuple contain a HahMap or Java object. Key is string, Value can be any type derived from Java object. <br>
 * <br>
 * Ports:<br>
 * <b>Input</b>: This has a single input port that writes data into database.<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None <br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCTransactionHashMapOutputOperator<V> extends JDBCTransactionOutputOperator<HashMap<String, V>>
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCTransactionHashMapOutputOperator.class);

  /**
   * @param mapping
   */
  @Override
  protected void parseMapping(ArrayList<String> mapping)
  {
    parseHashMapColumnMapping(mapping);
  }

  /*
   * Bind tuple values into insert statements.
   * @param tuple
   */
  @Override
  public void processTuple(HashMap<String, V> tuple) throws SQLException
  {
    if (tuple.isEmpty()) {
      emptyTuple = true;
    }
    for (Map.Entry<String, V> e: tuple.entrySet()) {
      tableToInsertStatement.get(keyToTable.get(e.getKey())).setObject(
              keyToIndex.get(e.getKey()).intValue(),
              e.getValue(),
              getSQLColumnType(keyToType.get(e.getKey())));
    }
  }
}