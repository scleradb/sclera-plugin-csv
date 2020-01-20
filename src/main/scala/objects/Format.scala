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

import org.apache.commons.csv.CSVFormat

object Format {
    def apply(
        formatOpt: Option[String]
    ): CSVFormat = formatOpt.map {
        case "DEFAULT" => CSVFormat.DEFAULT
        case "EXCEL" => CSVFormat.EXCEL
        case "MYSQL" => CSVFormat.MYSQL
        case "RFC4180" => CSVFormat.RFC4180
        case "TDF" => CSVFormat.TDF
        case _ =>
            throw new IllegalArgumentException(
                "Unknown CSV format. Should be one of " +
                "\"DEFAULT\", \"EXCEL\", \"MYSQL\", \"RFC4180\" or \"TDF\"."
            )
    } getOrElse CSVFormat.DEFAULT
}
