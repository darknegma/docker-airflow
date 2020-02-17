import pytest
import json
import os
import sys
from pathlib import Path

full_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, str(Path(full_path).parents[2]))
from sdc_etl_libs.aws_helpers.S3Data import S3Data
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers

# 2.  Test retrieving the same record from two different calls.. show they are equal
# 3.  Test retrieving two different records show the are different
# 4.  Test getting record size
# 5.  Test retrieving record chunk size
# 6.  Test retrieving the schema data

bucket_name = 'sdc-marketing-vendors'
bucket_prefix = 'neustar/'
file_prefix = 'SmileDirect_Fact_Funnel_Extract'
file_suffix = '.gz'
region = 'us-west-1'
file = 'SmileDirect_Fact_Funnel_Extract_2019-08-24_to_2019-08-25.tsv.gz'

data_schema_file = SDCFileHelpers.get_file_path("schema", 'Neustar/neustar_fact_funnel.json')
df_schema = json.loads(open(data_schema_file).read())

# Instantiate our object
large_file = S3Data(bucket_name_=bucket_name,
           prefix_=bucket_prefix,
           file_=file,  # afile,
           df_schema_=df_schema,
           check_headers_= True,
           file_type_='tsv',
           compression_type_='gz',
           region_=region,
           decode_='utf-8')
large_file.load_data()


def test_same_record():
    r2 = large_file.get_records(15, 1,1)
    r1 = large_file.get_records(20, 1,1)  # Move pointer forward
    r4 = large_file.get_records(15, 1,1)

    assert r2 == r4
    assert r2 != r1


def test_get_record_count():
    filesize = large_file.get_file_record_count()

    assert filesize == 1613081


def test_get_chunk_size():
    r3 = large_file.get_records(35, 3225,1)

    assert len(r3) == 3225


def test_schema_data():
    table = large_file.table
    schema = large_file.schema

    assert table.upper() == 'NEUSTAR_FACT_FUNNEL'
    assert schema.upper() == 'NEUSTAR'

# test_same_record()
# test_get_record_count()
# test_get_chunk_size()
# test_schema_data()
