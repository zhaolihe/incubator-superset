# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# pylint: disable=C,R,W

import json

import requests

from superset.yoyi.utils import utils

class metadata:

    @classmethod
    def get_metadata_from_bi(self, bi_url, data):
        """
        从BI获取元数据
        :param data:
        :return:
        """
        url = "{}/bi/getMetadataByType?type=1&logicType=bi".format(bi_url)
        request = requests.get(url=url)
        obj = json.loads(request.content, encoding="utf-8")
        return self.build_metadata(obj, form_data=data)

    @staticmethod
    def build_metadata(metadata, form_data):
        all = []
        dims = []
        metrics_combo = []
        columns = []
        metrics = []
        verbose = {}
        order_by_choices = []
        for item in metadata:
            fieldName = item["fieldName"]
            displayName = item["displayName"]
            logicCategory = item["logicCategory"]
            dataType = item["dataType"]

            temp = [fieldName, displayName]
            verbose[fieldName] = displayName
            all.append(temp)
            order_by_choices.append((json.dumps([fieldName, True]), "{} 正序".format(displayName)))
            order_by_choices.append((json.dumps([fieldName, False]), "{} 倒序".format(displayName)))

            if logicCategory == "dimension":
                dims.append(temp)
                desc = {
                    "type": dataType,
                    "dimension": fieldName,
                    "outputName": fieldName,
                    "outputType": dataType
                }
                columns.append({
                    "column_name": fieldName,
                    "verbose_name": displayName,
                    "description": displayName,
                    "expression": json.dumps(desc),
                    "filterable": True,
                    "groupby": True,
                    "is_dttm": None if fieldName != "bizdate" else True,
                    "type": None
                })
            elif logicCategory == "metric":
                metrics_combo.append(temp)
                desc = {
                    "type": "longSum",
                    "name": fieldName,
                    "fieldName": fieldName
                }
                metrics.append({
                    "metric_name": fieldName,
                    "verbose_name": displayName,
                    "description": displayName,
                    "expression": json.dumps(desc),
                    "warning_text": "",
                    "d3format": ""
                })
        form_data["all_cols"] = all
        form_data["filterable_cols"] = dims
        form_data["gb_cols"] = dims
        form_data["metrics_combo"] = metrics_combo
        form_data["name"] = utils.get_table_name()
        form_data["datasource_name"] = utils.get_table_name()
        form_data["columns"] = columns
        form_data["metrics"] = metrics
        form_data["verbose_map"] = verbose
        form_data["order_by_choices"] = order_by_choices
        return form_data
