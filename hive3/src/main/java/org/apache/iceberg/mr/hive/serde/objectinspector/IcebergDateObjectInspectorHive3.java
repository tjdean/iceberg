/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.serde.objectinspector;

import java.time.LocalDate;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.iceberg.util.DateTimeUtil;

public final class IcebergDateObjectInspectorHive3 extends AbstractPrimitiveJavaObjectInspector
    implements DateObjectInspector {

  private static final IcebergDateObjectInspectorHive3 INSTANCE = new IcebergDateObjectInspectorHive3();

  public static IcebergDateObjectInspectorHive3 get() {
    return INSTANCE;
  }

  private IcebergDateObjectInspectorHive3() {
    super(TypeInfoFactory.dateTypeInfo);
  }

  @Override
  public Date getPrimitiveJavaObject(Object o) {
    if (o == null) return null;
    else if (o instanceof LocalDate) return Date.ofEpochDay(DateTimeUtil.daysFromDate((LocalDate) o));
    else if (o instanceof Integer) return Date.ofEpochDay((Integer) o);
    else throw new IllegalStateException(String.format(
              "Expected type of %s or %s, but found %s",
              LocalDate.class,
              Integer.class,
              o.getClass().getName()));
  }

  @Override
  public DateWritableV2 getPrimitiveWritableObject(Object o) {
    if (o == null) return null;
    else if (o instanceof LocalDate) return new DateWritableV2(DateTimeUtil.daysFromDate((LocalDate) o));
    else if (o instanceof Integer) return new DateWritableV2((Integer) o);
    else throw new IllegalStateException(String.format(
            "Expected type of %s or %s, but found %s",
              LocalDate.class,
              Integer.class,
              o.getClass().getName()));
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) return null;
    else if (o instanceof Date) return new Date((Date) o);
    else if (o instanceof Integer) return LocalDate.ofEpochDay((Integer) o);
    else throw new IllegalStateException(String.format(
              "Expected type of %s or %s, but found %s",
              Date.class,
              Integer.class,
              o.getClass().getName()));
  }

}
