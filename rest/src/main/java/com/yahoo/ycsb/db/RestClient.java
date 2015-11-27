/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file. 
 */

package com.yahoo.ycsb.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource.Builder;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced
 * with YCSB. This class extends {@link DB} and implements the database
 * interface used by YCSB client.
 * 
 * <br>
 * Each client will have its own instance of this class. This client is not
 * thread safe.
 * 
 * <br>
 * This interface expects a schema <key> <field1> <field2> <field3> ... All
 * attributes are of type VARCHAR. All accesses are through the primary key.
 * Therefore, only one index on the primary key is needed.
 * 
 * <p>
 * The following options must be passed when using this database client.
 * 
 * <ul>
 * <li><b>db.driver</b> The JDBC driver class to use.</li>
 * <li><b>db.url</b> The Database connection URL.</li>
 * <li><b>db.user</b> User name for the connection.</li>
 * <li><b>db.passwd</b> Password for the connection.</li>
 * </ul>
 * 
 * @author sudipto
 *
 */
public class RestClient extends DB implements JdbcDBClientConstants {

	private ArrayList<Builder> resources;
	private boolean initialized = false;
	private Properties props;
	private static final String SQL_VARIABLE = "?";
	private static final String DEFAULT_PROP = "";
	private ConcurrentMap<StatementType, String> cachedStatements;

	/**
	 * The statement type for the prepared statements.
	 */
	private static class StatementType {

		enum Type {
			INSERT(1), DELETE(2), READ(3), UPDATE(4), SCAN(5),;
			int internalType;

			private Type(int type) {
				internalType = type;
			}

			int getHashCode() {
				final int prime = 31;
				int result = 1;
				result = prime * result + internalType;
				return result;
			}
		}

		Type type;
		int shardIndex;
		int numFields;
		String tableName;

		StatementType(Type type, String tableName, int numFields, int _shardIndex) {
			this.type = type;
			this.tableName = tableName;
			this.numFields = numFields;
			this.shardIndex = _shardIndex;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + numFields + 100 * shardIndex;
			result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
			result = prime * result + ((type == null) ? 0 : type.getHashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StatementType other = (StatementType) obj;
			if (numFields != other.numFields)
				return false;
			if (shardIndex != other.shardIndex)
				return false;
			if (tableName == null) {
				if (other.tableName != null)
					return false;
			} else if (!tableName.equals(other.tableName))
				return false;
			if (type != other.type)
				return false;
			return true;
		}
	}

	/**
	 * For the given key, returns what shard contains data for this key
	 *
	 * @param key
	 *            Data key to do operation on
	 * @return Shard index
	 */
	private int getShardIndexByKey(String key) {
		int ret = Math.abs(key.hashCode()) % resources.size();
		// System.out.println(conns.size() + ": Shard instance for "+ key + "
		// (hash " + key.hashCode()+ " ) " + " is " + ret);
		return ret;
	}

	/**
	 * For the given key, returns Builder object that holds connection to the
	 * shard that contains this key
	 *
	 * @param key
	 *            Data key to get information for
	 * @return Builder object
	 */
	private Builder getShardConnectionByKey(String key) {
		return resources.get(getShardIndexByKey(key)).accept(MediaType.APPLICATION_JSON)
				.type(MediaType.APPLICATION_JSON);
	}

	private void cleanupAllConnections() throws SQLException {
		// NO need to cleanup connections
	}

	/**
	 * Initialize the jersey client and set it up for sending requests to the
	 * REST Endpoint. This must be called once per client. @throws
	 */
	@Override
	public void init() throws DBException {
		if (initialized) {
			System.err.println("Client connection already initialized.");
			return;
		}
		props = getProperties();
		String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);

		try {
			int shardCount = 0;
			resources = new ArrayList<Builder>(3);
			for (String url : urls.split(",")) {
				System.out.println("Adding shard node URL: " + url);
				Client client = Client.create();
				Builder webResource = client.resource(url).accept(MediaType.APPLICATION_JSON)
						.type(MediaType.APPLICATION_JSON);

				shardCount++;
				resources.add(webResource);
			}

			System.out.println("Using " + shardCount + " shards");

			cachedStatements = new ConcurrentHashMap<StatementType, String>();
		} catch (NumberFormatException e) {
			System.err.println("Invalid value for fieldcount property. " + e);
			throw new DBException(e);
		}
		initialized = true;
	}

	@Override
	public void cleanup() throws DBException {
		try {
			cleanupAllConnections();
		} catch (SQLException e) {
			System.err.println("Error in closing the connection. " + e);
			throw new DBException(e);
		}
	}

	private String createAndCacheReadStatement(StatementType readType, String key) throws SQLException {
		StringBuilder read = new StringBuilder("SELECT * FROM ");
		read.append(readType.tableName);
		read.append(" WHERE ");
		read.append(PRIMARY_KEY);
		read.append(" = ");
		read.append("'?'");
		String readStatement = read.toString();
		String stmt = cachedStatements.putIfAbsent(readType, readStatement);
		if (stmt == null)
			return readStatement;
		else
			return stmt;
	}

	private String createAndCacheScanStatement(StatementType scanType, String key) throws SQLException {
		StringBuilder select = new StringBuilder("SELECT * FROM ");
		select.append(scanType.tableName);
		select.append(" WHERE ");
		select.append(PRIMARY_KEY);
		select.append(" >= '?'");
		select.append(" ORDER BY ");
		select.append(PRIMARY_KEY);
		select.append(" LIMIT '?'");

		String scanStatement = select.toString();
		String stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
		if (stmt == null)
			return scanStatement;
		else
			return stmt;
	}

	@Override
	public Status read(String tableName, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		try {
			StatementType type = new StatementType(StatementType.Type.READ, tableName, 1, getShardIndexByKey(key));
			String readStatement = cachedStatements.get(type);
			if (readStatement == null) {
				readStatement = createAndCacheReadStatement(type, key);
			}
			readStatement = StringUtils.replace(readStatement, SQL_VARIABLE, key);
			Builder client = getShardConnectionByKey(key);
			ClientResponse response = client.post(ClientResponse.class, readStatement);

			if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
				throw new SQLException("Query " + readStatement + " encountered an error ");
			}

			JSONArray jsonArray = response.getEntity(JSONArray.class);
			response.close();

			if (jsonArray.length() == 0) {
				return Status.NOT_FOUND;
			}
			if (result != null && fields != null) {
				for (String field : fields) {
					String value = jsonArray.getJSONObject(0).getString(field);
					result.put(field, new StringByteIterator(value));
				}
			}
			return Status.OK;
		} catch (SQLException e) {
			System.err.println("Error in processing read of table " + tableName + ": " + e);
			return Status.ERROR;
		} catch (JSONException e) {
			System.err.println("Error in parsing the results " + e);
			return Status.ERROR;
		} catch (ClientHandlerException | UniformInterfaceException e) {
			System.err.println("Error in reading results into JSON: " + e);
			return Status.ERROR;
		}
	}

	@Override
	public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result) {
		try {
			StatementType type = new StatementType(StatementType.Type.SCAN, tableName, 1, getShardIndexByKey(startKey));
			String scanStatement = cachedStatements.get(type);
			if (scanStatement == null) {
				scanStatement = createAndCacheScanStatement(type, startKey);
			}

			scanStatement = StringUtils.replaceOnce(scanStatement, SQL_VARIABLE, startKey);
			scanStatement = StringUtils.replaceOnce(scanStatement, SQL_VARIABLE, Integer.toString(recordcount));

			Builder client = getShardConnectionByKey(startKey);
			ClientResponse response = client.post(ClientResponse.class, scanStatement);

			if (response.getClientResponseStatus() != com.sun.jersey.api.client.ClientResponse.Status.OK) {
				throw new SQLException("Query {} encountered an error ", scanStatement);
			}

			JSONArray jsonArray = response.getEntity(JSONArray.class);

			for (int i = 0; i < recordcount && i < jsonArray.length(); i++) {
				if (result != null && fields != null) {
					HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
					for (String field : fields) {
						String value = jsonArray.getJSONObject(i).getString(field);
						values.put(field, new StringByteIterator(value));
					}
					result.add(values);
				}
			}
			return Status.OK;
		} catch (SQLException e) {
			System.err.println("Error in processing scan of table: " + tableName + e);
			return Status.ERROR;
		} catch (JSONException e) {
			System.err.println("Error in parsing the results " + e);
			return Status.ERROR;
		}
	}

	@Override
	public Status update(String table, String key, HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status delete(String table, String key) {
		// TODO Auto-generated method stub
		return null;
	}

}
