import json
from datetime import timedelta

import pandas
import requests

from superset import app
from superset.yoyi.metadata import metadata
from superset.yoyi.filter_handler import filter_handler
from superset.yoyi.utils import utils

DTTM_ALIAS = '__timestamp'


class query:
    host = "http://api.bi.data.yoyi.tv:8080/bi-query/api"

    @classmethod
    def get_metadata_from_bi(self, data):
        clazz = metadata()
        return clazz.get_metadata_from_bi(bi_url=self.host, data=data)

    @classmethod
    def query(self, query_obj):
        json_bi = self.build_query_info(query_obj)
        app.logger.info(json_bi)
        result = self.query_bi(json_bi)
        app.logger.info(json.dumps(result))
        data = result["data"]["data"]
        time_fields = utils.parse_time_field(query_obj["granularity"])
        if time_fields:
            for item in data:
                for dim in time_fields:
                    item[DTTM_ALIAS] = item[dim]
        df = pandas.DataFrame(data)
        return df, result["query"]

    @classmethod
    def query_bi(self, json_data):
        url = "{}/bi/getQuery".format(self.host)
        headers = {"content-type": "application/json"}
        response = requests.post(url=url, data=json_data, headers=headers)
        return json.loads(response.text, encoding="utf-8")

    @classmethod
    def build_query_info(self, query_obj):
        time_fields = utils.parse_time_field(query_obj["granularity"])
        fields = set.union(set(time_fields))
        fields = list(fields.union(set(query_obj["groupby"])).union(set(query_obj["metrics"])))
        if "columns" in query_obj.keys():
            fields = list(set(query_obj["columns"]).union(set(fields)))
        since, until = self.parse_time_range(query_obj["from_dttm"], query_obj["to_dttm"])
        orderbys = query_obj["orderby"] if  "orderby" in query_obj else []

        handler = filter_handler()
        filters = handler.build_filters(query_obj["filter"])
        queryEntity = {
            "type": "entityQuery",
            "logicType": "superset",
            "owner": "dsp",
            "queryFields": fields,
            "since": since,
            "until": until,
            "filters": filters,
            "pageInfo": {
                "pageIndex": 0,
                "pageSize": query_obj["row_limit"] if "row_limit" in query_obj else 100,
                "hasTotalCount": False
            },
            "orderBys": query.parse_oder_by(orderbys)
        }
        return json.dumps(queryEntity)

    @staticmethod
    def parse_time_range(from_dttm, to_dttm):
        d_format = "%Y-%m-%d"
        since = from_dttm.strftime(d_format)
        temp = to_dttm if (to_dttm.__eq__(from_dttm)) else to_dttm + timedelta(days=-1)
        until = temp.strftime(d_format)
        return (since, until)

    @staticmethod
    def parse_oder_by(orderbys):
        index = 1
        orders = []
        for k, v in orderbys:
            info = {
                "orderBy": k,
                "orderIndex": index,
                "ascending": v
            }
            orders.append(info)
            index += 1
        return orders
