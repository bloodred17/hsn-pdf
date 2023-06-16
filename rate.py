
class Rate:
    value: float
    unit: str
    max: 'Rate'
    min: 'Rate'

    def __init__(self, value: float, unit: str):
        self.value = value
        self.unit = unit

    def _repr_(self):
        return str(self.value) + self.unit

    def _str_(self):
        return str(self.value) + self.unit

    def get_dict(self):
        result = {
            "value": self.value,
            "unit": self.unit,
        }
        if self.__dict__.get("max"):
            result["max"] = self.max.get_dict()
        if self.__dict__.get("min"):
            result["min"] = self.min.get_dict()
        return result
