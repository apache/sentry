/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.tests.e2e.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * This class holds ResultSet after query sentry privileges
 * header: contain result header information, which is a array of string
 * privilegeResultSet: contain privilege results from query
 */
public class PrivilegeResultSet {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(PrivilegeResultSet.class);

    protected int colNum = 0;
    protected List<String> header;
    protected List<ArrayList<String>> privilegeResultSet;

    public PrivilegeResultSet() {
        header = new ArrayList<String>();
        privilegeResultSet = new ArrayList<ArrayList<String>>();
    }

    public PrivilegeResultSet(Statement stmt, String query) {
        LOGGER.info("Getting result set for " + query);
        this.header = new ArrayList<String>();
        this.privilegeResultSet = new ArrayList<ArrayList<String>>();
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            this.colNum = rsmd.getColumnCount();
            for (int i = 1; i <= this.colNum; i++) {
                this.header.add(rsmd.getColumnName(i).trim());
            }
            while (rs.next()) {
                ArrayList<String> row = new ArrayList<String>();
                for (int i = 1; i <= colNum; i++) {
                    row.add(rs.getString(i).trim());
                }
                this.privilegeResultSet.add(row);
            }
        } catch (Exception ex) {
            LOGGER.info("Exception when executing query: " + ex);
        } finally {
            try {
                rs.close();
            } catch (Exception ex) {
                LOGGER.error("failed to close result set: " + ex.getStackTrace());
            }
        }
    }

    public List<ArrayList<String>> getResultSet() {
        return this.privilegeResultSet;
    }

    public List<String> getHeader() {
        return this.header;
    }

    /**
     * Given a column name, validate if one of its values equals to given colVal
     */
    public boolean verifyResultSetColumn(String colName, String colVal) {
        for (int i = 0; i < this.colNum; i ++) {
            if (this.header.get(i).equalsIgnoreCase(colName)) {
                for (int j = 0; j < this.privilegeResultSet.size(); j ++) {
                    if (this.privilegeResultSet.get(j).get(i).equalsIgnoreCase(colVal)) {
                        LOGGER.info("Found " + colName + " contains a value = " + colVal);
                        return true;
                    }
                }
            }
        }
        LOGGER.error("Failed to detect " + colName + " contains a value = " + colVal);
        return false;
    }

    /**
     * Unmarshall ResultSet into a string
     */
    @Override
    public String toString() {
        String prettyPrintString = new String("\n");
        for (String h : this.header) {
            prettyPrintString += h + ",";
        }
        prettyPrintString += "\n";
        for (ArrayList<String> row : this.privilegeResultSet) {
            for (String val : row) {
                if (val.isEmpty()) {
                    val = "null";
                }
                prettyPrintString += val + ",";
            }
            prettyPrintString += "\n";
        }
        return prettyPrintString;
    }
}
