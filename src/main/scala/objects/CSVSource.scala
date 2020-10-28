/**
* Sclera - CSV
* Copyright 2012 - 2020 Sclera, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.scleradb.plugin.datasource.csv.objects

import com.scleradb.sql.datatypes.Column
import com.scleradb.external.objects.ExternalSource

import com.scleradb.plugin.datasource.csv.result.CSVResult

/** CSV data source
  *
  * @param name Identifier of the data service - used in explain
  * @param urlStr CSV URL or file name or enclosing directory
  * @param formatOpt CSV file format (optional)
  * @param isHeaderPresent Is header present?
  * @param urlColOpt Optional column name, to contain the file name/URL
  * @param sourcePatterns List of patterns to filter source path names
  */
class CSVSource(
    override val name: String,
    urlStr: String,
    formatOpt: Option[String],
    isHeaderPresent: Boolean,
    urlColOpt: Option[String],
    sourcePatterns: List[String]
) extends ExternalSource {
    /** CSVResult object to generate the result */
    override val result: CSVResult = new CSVResult(
        urlStr, formatOpt, isHeaderPresent, urlColOpt, sourcePatterns
    )

    /** Columns of the result (virtual table)
      * obtained from the first line (header) of the CSV file.
      * Each column has type CHAR VARYING.
      */
    override val columns: List[Column] = result.columns

    /** String used for this source in the EXPLAIN output */
    override def toString: String = "%s(\"%s\")".format(name, urlStr)
}
