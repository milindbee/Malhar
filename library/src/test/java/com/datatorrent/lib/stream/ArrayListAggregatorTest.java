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
package com.datatorrent.lib.stream;

import com.datatorrent.lib.stream.ArrayListAggregator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Functional test for {@link com.datatorrent.lib.testbench.ArrayListAggregator}
 */
public class ArrayListAggregatorTest
{
	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
	public void testNodeProcessing() throws Exception
	{
		ArrayListAggregator<Integer> oper = new ArrayListAggregator<Integer>();
		CollectorTestSink cSink = new CollectorTestSink();

		oper.output.setSink(cSink);
		oper.setSize(10);
		int numtuples = 100;

		oper.beginWindow(0);
		for (int i = 0; i < numtuples; i++) {
			oper.input.process(i);
		}
		oper.endWindow();
		Assert.assertEquals("number emitted tuples", 10,
				cSink.collectedTuples.size());

		cSink.clear();
		oper.setSize(0);

		oper.beginWindow(1);
		for (int i = 0; i < numtuples; i++) {
			oper.input.process(i);
		}
		oper.endWindow();
		Assert.assertEquals("number emitted tuples", 1,
				cSink.collectedTuples.size());
		ArrayList<?> list = (ArrayList<?>) cSink.collectedTuples.get(0);
		Assert.assertEquals("number emitted tuples", numtuples, list.size());
	}
}
