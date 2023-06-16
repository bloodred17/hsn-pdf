

class Heading:
    code: str
    heading_type: str

    def __init__(self, code: str, heading_type: str):
        self.code = code
        self.heading_type = heading_type

    def get_dict(self):
        return self.__dict__
