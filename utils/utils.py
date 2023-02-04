import uuid


class Utils:

    @staticmethod
    def convert_int_to_uuid_version_4(value: int):
        return uuid.UUID(int=value, version=4)
