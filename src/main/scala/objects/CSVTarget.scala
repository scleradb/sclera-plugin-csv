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

import scala.language.postfixOps

import java.io.{File, FileWriter}

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import com.scleradb.external.objects.ExternalTarget
import com.scleradb.sql.expr.{CharConst, DateConst, TimeConst, TimestampConst}
import com.scleradb.sql.result.TableResult

/** CSV data target
  *
  * @param name Identifier of the data service - used in explain
  * @param fileName CSV file name or enclosing directory
  * @param formatOpt CSV file format (optional)
  */
class CSVTarget(
    override val name: String,
    fileName: String,
    formatOpt: Option[String]
) extends ExternalTarget {
    private val format: CSVFormat = Format(formatOpt)

    override def write(ts: TableResult): Unit = {
        val writer: CSVPrinter =
            format.withHeader(
                ts.columns.map { col => col.name }: _*
            ).print(new FileWriter(new File(fileName)))

        // rows
        ts.rows.foreach { row =>
            writer.printRecord(
                ts.columns.map { col =>
                    row.getScalValueOpt(col.name, col.sqlType).map {
                        case CharConst(s) => s
                        case DateConst(v) => v.toString
                        case TimeConst(v) => v.toString
                        case TimestampConst(v) => v.toString
                        case other => other.repr
                    } getOrElse ""
                }: _*
            )
        }

        writer.close()
    }

    /** String used for this target in the EXPLAIN output */
    override def toString: String = "%s(\"%s\")".format(name, fileName)

    /** Serialize a proxy containing only the parameters */
    def writeReplace(): java.lang.Object =
        new SerializedCSVTarget(
            name,
            fileName,
            formatOpt
        )
}

/** Proxy object used for serialization - contains only the parameters */
class SerializedCSVTarget(
    name: String,
    fileName: String,
    formatOpt: Option[String]
) extends java.io.Serializable {
    /** Construct the CSV target object from the retrieved parameters */
    def readResolve(): java.lang.Object =
        new CSVTarget(
            name,
            fileName,
            formatOpt
        )
}
