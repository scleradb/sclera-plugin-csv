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

package com.scleradb.plugin.datasource.csv.result

import java.io.IOException

import scala.jdk.CollectionConverters._

import com.scleradb.sql.types.SqlCharVarying
import com.scleradb.sql.expr.{SortExpr, CharConst}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow}

import com.scleradb.util.io.ContentIter

import com.scleradb.plugin.datasource.csv.objects.CSVSource

/** Wrapper over the Apache Commons CSV parser object.
  * Generates a table containing the contents of a CSV file.
  *
  * @param urlStr CSV URL or file name or enclosing directory
  * @param formatOpt CSV file format (optional)
  * @param isHeaderPresent Is header present?
  * @param urlColOpt Optional column name, to contain the file name/URL
  * @param sourcePatterns List of patterns to filter source path names
  */
class CSVResult(
    urlStr: String,
    formatOpt: Option[String],
    isHeaderPresent: Boolean,
    urlColOpt: Option[String],
    sourcePatterns: List[String]
) extends TableResult {
    /** CSV records are not assumed to be sorted */
    override val resultOrder: List[SortExpr] = Nil

    /** Fetches content from URL / files / compressed archives */
    private val contentIter: ContentIter = ContentIter(sourcePatterns)

    /** Fetches CSV records from the content */
    private val recordIter: CSVRecordIter =
        new CSVRecordIter(contentIter.iter(urlStr), formatOpt, isHeaderPresent)

    /** Header information, read from the source CSV file.
      * If no headers appear in the CSV file read, tries to generate
      * generic headers COL1, COL2, ... from the data records.
      * Throws IOException if neither header nor data found.
      */
    def headers(): List[String] = recordIter.headersOpt() getOrElse {
        recordIter.lookAheadRecordOpt().map { record =>
            record.zipWithIndex.toList.map { case (_, i) => s"COL$i" }
        } getOrElse {
            throw new IOException("No header or data found")
        }
    }

    /** Columns of the result (virtual table)
      * obtained from the first line (header) of the CSV file.
      * Each column has type CHAR VARYING.
      */
    override lazy val columns: List[Column] = {
        val cols: List[String] = urlColOpt match {
            case Some(urlCol) => urlCol::headers()
            case None => headers()
        }

        cols.map { col => Column(col,  SqlCharVarying(None)) }
    }

    /** Reads the CSV file and emits the data as an iterator on rows.
      * Each row contains the (column-name -> value) pairs.
      */
    override def rows: Iterator[ScalTableRow] =
        recordIter.map { case (url, vals) =>
            val headerColVals: List[(String, CharConst)] =
                headers().zip(vals).map { case (col, v) =>
                    col -> CharConst(v.trim)
                }

            val colVals: List[(String, CharConst)] = urlColOpt match {
                case Some(urlCol) => (urlCol -> CharConst(url))::headerColVals
                case None => headerColVals
            }

            ScalTableRow(colVals)
        }

    /** Free resources held while iterating over the source content files */
    override def close(): Unit = {
        recordIter.close()
        contentIter.close()
    }
}
