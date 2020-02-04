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

import org.apache.commons.csv.{CSVRecord, CSVParser}

import scala.jdk.CollectionConverters._

import com.scleradb.sql.types.SqlCharVarying
import com.scleradb.sql.expr.{SortExpr, CharConst}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow}

import com.scleradb.plugin.datasource.csv.objects.CSVSource

/** Wrapper over the Apache Commons CSV parser object.
  * Generates a table containing the contents of a CSV file.
  *
  * @param csvSource CSVSource object
  * @param csvReaderIterator Iterator on CSV readers
  */
class CSVResult(
    csvSource: CSVSource,
    csvReaderIter: Iterator[(String, CSVParser)]
) extends TableResult {
    override val resultOrder: List[SortExpr] = Nil

    /** Columns of the result (virtual table)
      * obtained from the first line (header) of the CSV file.
      * Each column has type CHAR VARYING.
      */
    override val columns: List[Column] = csvSource.columns

    private var curCsvReaderOpt: Option[(CSVParser, Iterator[CSVRecord])] = None
    private var curUrlColValOpt: Option[(String, CharConst)] = None

    /** Reads the CSV file and emits the data as an iterator on rows.
      * Each row contains the (column-name -> value) pairs.
      */
    override def rows: Iterator[ScalTableRow] = new Iterator[ScalTableRow] {
        private var nextRowOpt: Option[CSVRecord] = None

        override def hasNext: Boolean = nextRowOpt match {
            case Some(_) => true
            case None =>
                curCsvReaderOpt.foreach { case (reader, _) => reader.close() }
                curCsvReaderOpt = None

                if( csvReaderIter.hasNext ) {
                    val (url, csvReader) = csvReaderIter.next()
                    val it: Iterator[CSVRecord] = csvReader.iterator().asScala

                    curCsvReaderOpt = Some((csvReader, it))
                    curUrlColValOpt = csvSource.urlColOpt.map { col =>
                        col -> CharConst(url.toString)
                    }

                    if( it.hasNext ) {
                        nextRowOpt = Option(it.next())
                    }

                    hasNext
                } else false
        }

        override def next(): ScalTableRow = if( hasNext ) {
            val vals: Iterator[String] = nextRowOpt.get.iterator().asScala
                    
            nextRowOpt = curCsvReaderOpt.flatMap { case (_, it) =>
                if( it.hasNext ) Some(it.next()) else None
            }

            val headerColVals: List[(String, CharConst)] =
                csvSource.headerColumns.zip(vals.toList).map {
                    case (col, v) => (col.name -> CharConst(v.trim))
                }

            val colVals: List[(String, CharConst)] = curUrlColValOpt match {
                case Some(urlColVal) => urlColVal::headerColVals
                case None => headerColVals
            }

            ScalTableRow(colVals)
        } else Iterator.empty.next()
    }

    /** Closes the reader */
    override def close(): Unit = {
        curCsvReaderOpt.foreach { case (reader, _) => reader.close() }
        csvSource.close()
    }
}
