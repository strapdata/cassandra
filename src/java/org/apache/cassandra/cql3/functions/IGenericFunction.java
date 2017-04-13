/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.functions;

import org.apache.commons.lang3.reflect.FieldUtils;

/**
 * CQL function taking arbitrary types.
 *
 */
public interface IGenericFunction extends Function 
{
    /**
     * GenericFunction should declare a public static class implementing IGenericFunction.Loader with a public constructor with no arguments.
     * Then, by declaring this inner class in META-INF/services/org.apache.cassandra.cql3.functions.IGenericFunction$Loader, 
     * generic function will be registred by a ServiceLoader<IGenericFunction.Loader> in GenericFunctionRegistry.<clinit>.
     * This allows to drop a jar defining custom generic CQL functions in the Cassandra classpath.
     */
    public interface Loader 
    {
        public default void register(Class<IGenericFunction> clazz) throws IllegalAccessException 
        {
            // generic function should always have a public static FunctionName NAME field.
            FunctionName functionName = (FunctionName) FieldUtils.readDeclaredStaticField(clazz, "NAME");
            GenericFunctionRegistry.registerFunction(functionName, clazz);
        }
        
        // could be overridden if Loader in not an inner class of the generic function.
        public default Class getGenericFunctionClass() 
        {
            return this.getClass().getDeclaringClass();
        }
    }
    
}
