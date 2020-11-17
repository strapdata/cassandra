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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry for generic CQL functions.
 *
 */
public class GenericFunctionRegistry 
{
    private static final Logger logger = LoggerFactory.getLogger(GenericFunctionRegistry.class);
    
    private static GenericFunctionRegistry instance = new GenericFunctionRegistry();
    
    // Register generic functions available in the classpath and declared in META-INF/service/org.apache.cassandra.cql3.functions.IGenericFunction$Loader
    static {
        try 
        {
            for(Iterator<IGenericFunction.Loader> loaderIt = ServiceLoader.load(IGenericFunction.Loader.class).iterator(); loaderIt.hasNext(); ) 
            {
                IGenericFunction.Loader functionLoader = loaderIt.next();
                functionLoader.register( functionLoader.getGenericFunctionClass() );
            }
        } catch (ServiceConfigurationError | IllegalAccessException e) 
        {
            logger.error("Failed to load generic functions:"+e.getMessage(), e);
        }
    }
    
    Map<String, Class<? extends IGenericFunction>> registry;
    
    private GenericFunctionRegistry() 
    {
        this.registry = new HashMap<String, Class<? extends IGenericFunction>>() 
        {{
           put(ToJsonFct.NAME.name(), ToJsonFct.class); 
        }};
    }
    
    public static IGenericFunction getInstance(FunctionName functionName, List<AbstractType<?>> argTypes) throws InvalidRequestException 
    {
        Class<? extends IGenericFunction> clazz = instance.registry.get(functionName.name());
        if (clazz != null) 
        {
            try 
            {
                Method m = clazz.getDeclaredMethod("getInstance", List.class);
                return (IGenericFunction) m.invoke(null, argTypes);
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) 
            {
                throw new InvalidRequestException(String.format("Failed to instanciate function '%s':"+e.getMessage(), functionName));
            }
        }
        return null;
    }
    
    public static boolean isGenericFunction(FunctionName functionName) 
    {
       return instance.registry.containsKey(functionName.name());
    }
    
    public static void registerFunction(FunctionName functionName, Class<? extends IGenericFunction> functionClass) 
    {
        instance.registry.put(functionName.name(), functionClass);
    }
}
