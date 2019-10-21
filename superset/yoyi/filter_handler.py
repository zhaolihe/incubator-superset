class filter_handler():
    @classmethod
    def build_filters(self, input):
        filters = []
        for info in input:
            vals = info["val"]
            if isinstance(vals, list):
                fv = vals
            else:
                fv = [vals]

            symbol = info["op"]
            operator = self.resolve_operator(symbol)
            filter = {
                "filterName": info["col"],
                "operator": operator,
                "filterValues": fv,
                "filterCategory": 1,
                "type": "filterItem"
            }
            filters.append(filter)
        return filters

    @classmethod
    def resolve_operator(self, symbol):
        if symbol == "==":
            return 1
        elif symbol == "in":
            return 5
        elif symbol == ">":
            return 8
        else:
            return 1