import json
import logging
from sdc_etl_libs.sdc_data_exchange.SDCDataExchangeEnums import DataExchangeTypes, \
    FileExchangeTypes, FileResultTypes, DatabaseExchangeTypes, APIExchangeTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers
from sdc_etl_libs.sdc_data_schema.SDCDataSchema import SDCDataSchema
from sdc_etl_libs.sdc_data_exchange.SDCDataExchangeEndpointFactory import \
    SDCDataExchangeEndpointFactory
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe
from sdc_etl_libs.sdc_file_helpers.SDCFile import SDCFile
from sdc_etl_libs.sdc_file_helpers.SDCCSVFile import SDCCSVFile
from sdc_etl_libs.sdc_file_helpers.SDCParquetFile import SDCParquetFile
from sdc_etl_libs.sdc_exchange_endpoints.database_endpoints.SDCSnowflakeEndpoint \
    import DataframeForDBEmpty, DataframeFailedLoading


class SDCDataExchange:

    def __init__(self, schema_name_, source_endpoint_tag_, sink_endpoint_tag_):
        """
        SDCDataExchange constructor. Creates source and sink instances and
        compare file lists between endpoints (if applicable to types).
        :param schema_name_: Name of data schema.
        :param source_endpoint_tag_: Tag of source endpoint (from data schema).
        :param sink_endpoint_tag_: Tag of source endpoint (from data schema).
        """

        self.source_endpoint_schema = None
        self.sink_endpoint_schema = None
        self.source = None
        self.sink = None
        self.source_endpoint_tag = source_endpoint_tag_
        self.sink_endpoint_tag = sink_endpoint_tag_
        self.etl_results_log = {}
        self.sinks = []
        self.sink_files_to_process = {}

        self.data_schema = json.loads(
            open(SDCFileHelpers.get_file_path(
                'schema', f"{schema_name_}.json")).read())

        self.source_endpoint_schema = SDCDataSchema.parse_endpoint_data(
            schema_data_json_=self.data_schema,
            endpoint_tag_=self.source_endpoint_tag)

        self.source = SDCDataExchangeEndpointFactory.get_endpoint(
            self.data_schema, self.source_endpoint_schema)

        if isinstance(sink_endpoint_tag_, list):
            for tag in sink_endpoint_tag_:
                ep_schema = SDCDataSchema.parse_endpoint_data(
                    schema_data_json_=self.data_schema,
                    endpoint_tag_=tag)

                sink = SDCDataExchangeEndpointFactory.get_endpoint(
                    self.data_schema, ep_schema)

                self.sinks.append(sink)

                self.sink_files_to_process[sink.endpoint_tag] = \
                    None if self.source.files is None else SDCDataExchange.compare_file_lists(self.source, sink)

        elif isinstance(sink_endpoint_tag_, str):
            ep_schema = SDCDataSchema.parse_endpoint_data(
                schema_data_json_=self.data_schema,
                endpoint_tag_=sink_endpoint_tag_)

            sink = SDCDataExchangeEndpointFactory.get_endpoint(
                self.data_schema, ep_schema)

            self.sinks.append(sink)

            self.sink_files_to_process[sink.endpoint_tag] = \
                None if self.source.files is None else SDCDataExchange.compare_file_lists(self.source, sink)

        else:
            raise Exception("Invalid sink tag")

        if self.source.files is not None:
            distinct_sink_files = set()
            for sink in self.sinks:
                distinct_sink_files.update(self.sink_files_to_process[sink.endpoint_tag])
            self.sink_files_to_process[self.source.endpoint_tag] = distinct_sink_files

    @staticmethod
    def compare_file_lists(source_, sink_):
        """
        Compares two file lists (source and sink) and returns the the difference
        (what's in source but not in sink).
        :param source_: Source endpoint class.
        :param sink_: Sink endpoint class.
        :return: List of filenames.
        """

        if not isinstance(source_.files, list) or \
                not isinstance(sink_.files, list):
            raise Exception(f"Must pass two lists. source_files_ was "
                            f"{type(source_.files)} and sink_files_ was "
                            f"{type(sink_.files)}")

        file_list = list(set(source_.files).difference(set(sink_.files)))

        return file_list

    def exchange_data(self):
        """
        Exchanges data between a source and a sink.
        :return: As ETL results log as a dictionary of source/sink file result
            records.
        """

        def log_empty_result():
            msg = f"{FileResultTypes.empty.value}: " \
                  f"{file_to_process} contains no data and was skipped."
            logging.error(f"{msg}")
            for sink in self.sinks:
                if file_to_process in self.sink_files_to_process[sink.endpoint_tag]:
                    self.etl_results_log[sink.endpoint_tag].append(msg)

        self.etl_results_log = {}

        if self.source_endpoint_schema["type"] in FileExchangeTypes.__members__:

            # Log message if no files exist to process for a sink.
            for sink in self.sinks:
                self.etl_results_log[sink.endpoint_tag] = []
                if len(self.sink_files_to_process[sink.endpoint_tag]) == 0:
                    msg = f"There were no new files to send to the {sink.exchange_type} sink."
                    logging.info(msg)
                    self.etl_results_log[sink.endpoint_tag].extend([msg])

            if self.sink_files_to_process[self.source.endpoint_tag]:
                num_files_to_process = len(self.sink_files_to_process[self.source.endpoint_tag])
                logging.info(f"There are {num_files_to_process} files(s) to process.")
                for file_no, file_to_process in enumerate(self.sink_files_to_process[self.source.endpoint_tag]):
                    logging.info(f"Processing {file_to_process} (File {file_no + 1}/{num_files_to_process})...")
                    source_data = None
                    try:
                        source_data = self.source.get_data(file_to_process)

                        # TODO Make the below it's own function so we can reuse

                        if isinstance(source_data, SDCCSVFile) or isinstance(source_data, SDCParquetFile):

                            temp_df = source_data.get_file_as_dataframe()
                            data_length = len(temp_df.df)

                            # Checking for empty files and log if empty.
                            if isinstance(source_data, Dataframe):
                                data_length = len(source_data.df)

                            if data_length == 0:
                                log_empty_result()
                                continue

                    except Exception as e:
                        msg = f"{FileResultTypes.error.value}: " \
                              f"Sourcing {file_to_process} from " \
                              f"{self.source.exchange_type} failed."
                        logging.warning(f"{msg}.")
                        for sink in self.sinks:
                            if file_to_process in self.sink_files_to_process[sink.endpoint_tag]:
                                self.etl_results_log[sink.endpoint_tag].append(msg)
                        continue

                    for sink in self.sinks:
                        if file_to_process in self.sink_files_to_process[sink.endpoint_tag]:
                            try:
                                result = sink.write_data(source_data, file_to_process)
                                msg = f"{FileResultTypes.success.value}: Loaded " \
                                      f"{file_to_process} to {sink.exchange_type}. {result}"
                                logging.info(msg)
                                self.etl_results_log[sink.endpoint_tag].append(msg)

                            except DataframeForDBEmpty:
                                log_empty_result()
                                continue

                            except Exception as e:
                                msg = f"{FileResultTypes.error.value}: " \
                                    f"Syncing {file_to_process} to " \
                                    f"{sink.exchange_type} failed."
                                logging.error(f"{msg}.")
                                self.etl_results_log[sink.endpoint_tag].append(msg)
                                continue

            return self.etl_results_log

        elif self.source_endpoint_schema["type"] in DatabaseExchangeTypes.__members__:
            sdc_df = self.source.get_data()
            for sink in self.sinks:
                self.etl_results_log[sink.endpoint_tag] = []
                try:
                    result = sink.write_data(sdc_df, sink.endpoint_schema["file_info"]["file_name"])
                    msg = f"{FileResultTypes.success.value}: Loaded " \
                          f"{sink.endpoint_schema['file_info']['file_name']} to {sink.exchange_type}. {result}"
                    logging.info(msg)
                    self.etl_results_log[sink.endpoint_tag].append(msg)

                except DataframeForDBEmpty:
                    log_empty_result()
                    continue

                except Exception as e:
                    msg = f"{FileResultTypes.error.value}: " \
                          f"Syncing {sink.endpoint_schema['file_info']['file_name']} to " \
                          f"{sink.exchange_type} failed."
                    logging.error(f"{msg}. {e}")
                    self.etl_results_log[sink.endpoint_tag].append(msg)
                    continue
            return self.etl_results_log

        elif self.source_endpoint_schema["type"] in APIExchangeTypes.__members__:
            # TODO: API as source needed
            pass

        try:
            self.source.close_connection()
        except Exception as e:
            pass
        try:
            self.sink.close_connecton()
        except Exception as e:
            pass
