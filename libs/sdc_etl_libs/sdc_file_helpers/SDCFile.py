

class SDCFile(object):
    type = None
    file_name = None
    file_path = None
    file_obj = None
    schema = None

    def __init__(self, schema_, endpoint_schema_, file_name_, file_path_, file_obj_):
        self.schema = schema_
        self.endpoint_schema = endpoint_schema_
        self.file_name = file_name_
        self.file_path = file_path_
        self.file_obj = file_obj_

        if self.endpoint_schema["file_info"]["type"] != "file":
            raise Exception("Base File class can only be used for generic files.")

    def get_file_size(self):
        pass

    def get_file_as_object(self):
        return self.file_obj

    def get_file_as_dataframe(self):
        raise Exception("Base file to dataframe not allowed.")