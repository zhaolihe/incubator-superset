class utils:

    @staticmethod
    def get_table_name():
        return "bi_metadata"

    @staticmethod
    def get_day_format():
        return "%Y-%m-%d"

    @staticmethod
    def is_yoyi_datasource(datasource_name, query_obj):
        return True if utils.get_table_name.__eq__(datasource_name) else False

    @staticmethod
    def get_data_format(query_obj):
        field = query_obj["granularity"]
        # 查询全周期数据
        if not field:
            return ""

        time_fileds = utils.parse_time_field(field)
        return "%H" if "action_hour" in time_fileds else utils.get_day_format()

    @staticmethod
    def parse_time_field(granularity):
        day = "action_day"
        hour = "action_hour"
        if not granularity:
            return []
        elif "one day" == granularity:
            return [day]
        elif "PT1H" == granularity:
            return [day, hour]
        else:
            return [day]
