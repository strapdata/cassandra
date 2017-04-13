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
