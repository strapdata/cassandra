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
package org.apache.cassandra.service;

import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.KeyspaceMetadata;

public abstract class MigrationListener
{
	/**
	 * Start a schema merge transaction.
	 * This helps to manage a batch of schema mutations together.
	 */
	public void onBeginTransaction()
	{
	}

	/**
	 * End a schema merge transaction.
	 */
	public void onEndTransaction()
	{
	}

    public void onCreateKeyspace(KeyspaceMetadata ksm)
    {
    	onCreateKeyspace(ksm.name);
    }

    public void onCreateKeyspace(String ksName)
    {
    }

    public void onCreateColumnFamily(KeyspaceMetadata ksm, CFMetaData cfm)
    {
    	onCreateColumnFamily(ksm.name, cfm.cfName);
    }

    public void onCreateColumnFamily(String ksName, String cfName)
    {
    }


    public void onCreateView(String ksName, String viewName)
    {
        onCreateColumnFamily(ksName, viewName);
    }

    public void onCreateUserType(String ksName, String typeName)
    {
    }

    public void onCreateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
    {
    }

    public void onCreateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
    {
    }

    public void onUpdateKeyspace(KeyspaceMetadata ksm)
    {
    	onUpdateKeyspace(ksm.name);
    }

    public void onUpdateKeyspace(String ksName)
    {
    }

    // the boolean flag indicates whether the change that triggered this event may have a substantive
    // impact on statements using the column family.
    public void onUpdateColumnFamily(KeyspaceMetadata ksm, CFMetaData cfm, boolean affectsStatements)
    {
    	onUpdateColumnFamily(ksm.name, cfm.cfName, affectsStatements);
    }

    public void onUpdateColumnFamily(String ksName, String cfName, boolean affectsStatements)
    {
    }

    public void onUpdateView(String ksName, String viewName, boolean columnsDidChange)
    {
        onUpdateColumnFamily(ksName, viewName, columnsDidChange);
    }

    public void onUpdateUserType(String ksName, String typeName)
    {
    }

    public void onUpdateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
    {
    }

    public void onUpdateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
    {
    }

    public void onDropKeyspace(KeyspaceMetadata ksm)
    {
    	onDropKeyspace(ksm.name);
    }

    public void onDropKeyspace(String ksName)
    {
    }

    public void onDropColumnFamily(KeyspaceMetadata ksm, CFMetaData cfm)
    {
    	onDropColumnFamily(ksm.name, cfm.cfName);
    }

    public void onDropColumnFamily(String ksName, String cfName)
    {
    }

    public void onDropView(String ksName, String viewName)
    {
        onDropColumnFamily(ksName, viewName);
    }

    public void onDropUserType(String ksName, String typeName)
    {
    }

    public void onDropFunction(String ksName, String functionName, List<AbstractType<?>> argTypes)
    {
    }

    public void onDropAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes)
    {
    }
}
