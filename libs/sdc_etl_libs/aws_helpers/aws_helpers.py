import csv
import json
import logging
import os,sys
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import re
from pathlib import Path
path = Path(__file__).parents[2]
sys.path.append(str(path))

logging.info(f"EXECUTING: {__name__}")
from sdc_etl_libs.aws_helpers.S3Data import S3Data

class AWSHelpers(object):

    @staticmethod
    def get_account_id() -> str:
        """
        Grab the AWS account id. Can be used to create resource ARN strings
        for use in other methods so that account id/ARN is not hard-coded.

        :return: Account ID as string
        """

        account_id = boto3.client("sts").get_caller_identity()["Account"]

        return str(account_id)

    @staticmethod
    def iterate_on_s3_response(response_: dict, bucket_name_: str,
                               prefix_: str, files_: list , give_full_path_):
        """
        Iterate over an S3 List Objects result and adds object file/object
        names to list.
        :param response_: Response from  List Objects func.
        :param bucket_name_: Name of S3 bucket that was searched.
        :param prefix_: Prefix used to find files.
        :param files_: List append S3 URLs to.
        :return: None
        """

        for item in response_["Contents"]:
            if prefix_ in item["Key"]:
                if give_full_path_:
                    files_.append("s3://" + bucket_name_ + "/" + item["Key"])
                else:
                    files_.append(os.path.basename(item["Key"]))

    @staticmethod
    def get_file_list_s3(bucket_name_, prefix_, access_key_=None,
                         secret_key_=None, region_='us-east-2',
                         file_prefix_: str = None, file_suffix_: str = None,
                         file_regex_: str = None, give_full_path_=False):
        """
        Creates a list of items in an S3 bucket.
        :param bucket_name_: Name of S3 bucket to search
        :param prefix_: Prefix used to find files.
        :param access_key_: AWS Access Key for S3 bucket (if applicable)
        :param secret_key_: AWS Secret Key for S3 bucket (if applicable)
        :param region_: AWS region (Default = 'us-east-2')
        :param file_prefix_: If used, function will return files that start
            with this (case-sensitive). Can be used in tandem with file_suffix_
        :param file_suffix_: If used, function will return files that end
            with this (case-sensitive). Can be used in tandem with file_prefix_
        :param file_regex_: If used, will return all files that match this
            regex pattern. file_prefix_ & file_suffix_ will be ignored.
        :param give_full_path_: If False, only file name will be returned. If
            true, full path & file name will be returned.
        :return: List of S3 file/object names as strings
        """

        client = boto3.client('s3', region_name=region_,
                              aws_access_key_id=access_key_,
                              aws_secret_access_key=secret_key_)

        response = client.list_objects_v2(Bucket=bucket_name_, Prefix=prefix_)

        all_files = []

        if "Contents" in response:
            AWSHelpers.iterate_on_s3_response(response, bucket_name_,
                                              prefix_, all_files, give_full_path_)
            while response["IsTruncated"]:
                print(response["NextContinuationToken"])
                response = client.list_objects_v2(
                    Bucket=bucket_name_, Prefix=prefix_,
                    ContinuationToken=response["NextContinuationToken"])
                AWSHelpers.iterate_on_s3_response(response, bucket_name_,
                                                  prefix_, all_files, give_full_path_)

            if file_regex_ or file_prefix_ or file_suffix_:
                pattern = file_regex_ if file_regex_ else \
                    f"{file_prefix_ if file_prefix_ else ''}.*{file_suffix_ if file_suffix_ else ''}"

                files = [x for x in all_files if re.search(pattern, x)]

            else:
                files = all_files

        return files

    @staticmethod
    def query_dynamodb_etl_table_for_files(table_name_, job_name_,
                                           region_='us-east-2'):
        '''
        @param table_name_: The name of the Dynamodb table
        @return: a dictionary of file names ex {"s3://file":1}.
        '''
        dynamodb = boto3.resource('dynamodb', region_name=region_)
        table = dynamodb.Table(table_name_)

        response = table.query(KeyConditionExpression=Key('job_name')
                               .eq(job_name_))
        data = response['Items']
        out = {}
        while 'LastEvaluatedKey' in response:
            try:
                response = table.query(
                    ExclusiveStartKey=response['LastEvaluatedKey'])
                data.append(response['Items'])
            except Exception as e:
                logging.error(e)
                logging.error("Failed to scan table %s" % (table_name_))

        for file in data:
            out[file["file_name"]] = 1

        return out

    @staticmethod
    def insert_processed_file_list_into_dynamo(table_, job_name_, file_list_,
                                               region_='us-east-2'):
        '''
        @param table_: Name of the Dynamodb Table
        @param data_: A list of Dictionary objects.
        @return: None
        '''

        insert_data = []

        for filename in file_list_:
            insert_data.append({"job_name": job_name_, "file_name": filename})

        dynamodb = boto3.resource('dynamodb', region_name=region_)
        table = dynamodb.Table(table_)
        try:
            with table.batch_writer() as batch:
                for item in insert_data:
                    batch.put_item(Item=item)
        except Exception as e:
            logging.error(e)
            logging.error("Failed to put %s" % (item))
            # need some retry logic?

    @staticmethod
    def add_item_to_dynamodb_table(table_,item_,region_='us-east-2',
                                    access_key_=None,secret_key_=None):
        '''
        @param table_: Name of the Dynamodb Table
        @param item_: item to be inserted
        @return: HTTP response
        '''
        dynamodb = boto3.resource('dynamodb', region_name=region_,
                                    aws_access_key_id=access_key_, aws_secret_access_key=secret_key_)
        table = dynamodb.Table(table_)

        try:
            response = table.put_item(Item=item_)
        except ClientError as e:
            logging.error(f"It's dead, Jimmy! {e}")
            raise
        except Exception as e:
            logging.error(f"Something terrible happened! All we know is {e}")
            raise
        return response["ResponseMetadata"]["HTTPStatusCode"]
    
    @staticmethod
    def get_item_from_dynamodb_table(table_,key_
                                    ,sortKey_="all",select_="all"
                                    ,consistentRead_=False,limit_=500
                                    ,access_key_=None,secret_key_=None
                                    ,region_='us-east-2'):
        """
        @param table_: Name of the Dynamodb Table
        @param key_: value to query table for
        @param sortKey_: is used to limit items within requested PartitionKey. 
                        Accepts:
                            ALL (default) - returns items for a given partition key
                            {VALUE} - returns items with a sort key matching passed value
                            FIRST - returns item with first sort key (as sorted in the table)
                            LAST - returns item with last sort key (as sorted in the table)
        @param select_: is used to limit which item's attributes (i.e. fields) to return
                        Accepts:
                            ALL (default) - returns all attributes
                            {VALUE} - returns one or more attributions specified in the parameter (e.g. "field1" or "field1,field2")
                            COUNT - returns number of matching items instead of matching items themselves
        @param consistentRead_: specifies whether the read should be performed with strong or eventual consistency.
                        Accepts:
                        False (default) - eventually consistent
                        True - strongly consistent
        @param limit_: specifies number of items to be evaluated(!) before returning the results
                        Accepts:
                            ALL - evaluates all records
                            {VALUE} - stops evaluating records at value. (default 500)
        @return: {statusCode:int,Payload[]}
        """
        dynamodb = boto3.resource('dynamodb', region_name=region_,
                                    aws_access_key_id=access_key_, aws_secret_access_key=secret_key_)

        table = dynamodb.Table(table_)
        key_schema = table.key_schema
        for k in key_schema:
            if k["KeyType"]=="HASH":
                HASH=k["AttributeName"]
            elif k["KeyType"]=="RANGE":
                RANGE=k["AttributeName"]

        condition = Key(HASH).eq(key_)
        scan_index_forward=True

        if sortKey_.lower()=="all":
            pass
        elif sortKey_.lower()=="first":
            limit_=1
        elif sortKey_.lower()=="last":
            limit_=1
            scan_index_forward=False
        elif (sortKey_.lower()).isnumeric():
            condition = Key(HASH).eq(key_) & Key(RANGE).eq(int(sortKey_))
        else:
            condition = Key(HASH).eq(key_) & Key(RANGE).eq(sortKey_)

        attributes_to_get=[]
        if select_.lower()=="all":
            select="ALL_ATTRIBUTES"
        elif select_.lower()=="count":
            select="COUNT"
        else:
            select="SPECIFIC_ATTRIBUTES"
            attributes_to_get=select_.split(",")

        try:
            if select != "SPECIFIC_ATTRIBUTES":
                response = table.query(
                    KeyConditionExpression=condition,
                    Limit=limit_,
                    ScanIndexForward=scan_index_forward,
                    Select=select,
                    ConsistentRead=consistentRead_
                )
            else:
                response = table.query(
                    KeyConditionExpression=condition,
                    Limit=limit_,
                    ScanIndexForward=scan_index_forward,
                    Select=select,
                    ProjectionExpression=select_,
                    ConsistentRead=consistentRead_
                )

        except ClientError as e:
            logging.error(e.response['Error']['Message'])
            resp={"statusCode":400,"Description":e.response['Error']['Message']}
            return resp
        else:
            resp={}
            resp["statusCode"]=response["ResponseMetadata"]["HTTPStatusCode"]
            if select=="COUNT":
                resp["Payload"] = response["Count"]
            else:
                resp["Payload"] = response['Items']
            return resp


    @staticmethod
    def delete_item_from_dynamodb_table(table_,key_,sortKey_="all",region_='us-east-2'
                                        ,access_key_=None,secret_key_=None):
        """
        @param table_: Name of the Dynamodb Table
        @param key_: partition key for the table
        @param sortKey_: range key for item to delete within PartitionKey. 
                        Accepts:
                            ALL (default) - returns items for a given partition key
                            {VALUE} - returns items with a sort key matching passed value
        @return: HTTP response code
        """

        dynamodb = boto3.resource('dynamodb', region_name=region_,
                                    aws_access_key_id=access_key_, aws_secret_access_key=secret_key_)
        table = dynamodb.Table(table_)
        key_schema = table.key_schema
        for k in key_schema:
            if k["KeyType"]=="HASH":
                HASH=k["AttributeName"]
            elif k["KeyType"]=="RANGE":
                RANGE=k["AttributeName"]

        key={
            HASH:key_
        }

        if sortKey_.lower()=="all":
            pass
        elif (sortKey_.lower()).isnumeric():
            key[RANGE]=int(sortKey_)
        else:
            key[RANGE]=sortKey_

        try:
            response = table.delete_item(
                Key=key
            )

        except ClientError as e:
            logging.error(e.response['Error']['Message'])
            raise
        else:
            return response["ResponseMetadata"]["HTTPStatusCode"]


    @staticmethod
    def get_secrets(secret_id_, access_key_=None,
                    secret_key_=None, region_='us-east-2'):
        """
        Retrieves AWS Secrets data.

        :param secret_id_: ID of secret
        :param access_key_: AWS Access Key (Optional)
        :param secret_key_: AWS Secret Key (Optional)
        :param region_: AWS Region
        :return: Dictionary containing results of secrets
        """

        try:
            secrets = boto3.client('secretsmanager', region_name=region_,
                                   aws_access_key_id=access_key_,
                                   aws_secret_access_key=secret_key_)

            response = secrets.get_secret_value(SecretId=secret_id_)
            secret_dict = json.loads(response["SecretString"])

            return secret_dict

        except Exception as e:
            logging.error("Failed to retrieve secrets. {}".format(e))

    @staticmethod
    def create_secrets(secret_name_, secret_desc_=None,
                       secret_string_=None, access_key_=None,
                       secret_key_=None, region_='us-east-2'):
        """
        Creates an AWS Secret.

        :param secret_name_: Name for the secret
        :param secret_desc_: Description for the secret (Optional)
        :param secret_string_: String in dictionary format containing the
            secret key/value pairs
        :param access_key_: AWS access key (Optional)
        :param secret_key_: AWS secret key (Optional)
        :param region_: AWS region (Default = 'us-east-2'
        :return: Dictionary containing results of secrets creation

        Example of secret_string_:
        {
            "username":"atticus",
            "password":"abc123"
        }
        """

        try:
            secrets = boto3.client('secretsmanager', region_name=region_,
                                   aws_access_key_id=access_key_,
                                   aws_secret_access_key=secret_key_)

            response = secrets.create_secret(
                Name=secret_name_,
                Description=secret_desc_,
                SecretString=secret_string_
            )

            return response

        except Exception as e:
            logging.error("Failed to create secret. {}".format(e))

    @staticmethod
    def sns_create_topic(topic_name_, access_key_=None,
                         secret_key_=None, region_='us-east-2'):
        """
        Create an SNS topic where messages can be sent.

        :param topic_name_: Desired name of SNS topic
        :param access_key_: AWS access key (Optional)
        :param secret_key_: AWS secret key (Optional)
        :param region_: AWS region
        :return: Dictionary of created SNS topic details
        """

        sns = boto3.client('sns', region_name=region_,
                           aws_access_key_id=access_key_,
                           aws_secret_access_key=secret_key_)
        response = sns.create_topic(Name=topic_name_)
        return response

    @staticmethod
    def sns_subscribe(topic_name_, protocol_='email', endpoint_='',
                      topic_arn_=None, access_key_=None, secret_key_=None,
                      region_='us-east-2'):
        """
        Subscribe to an SNS topic.

        :param topic_name_: Topic name (ex. 'prod-glue-tnt-events')
        :param protocol_: Method of delivery (Default = 'email')
        :param endpoint_: Where to send to (Delivery default is 'email',
            so email address here)
        :param access_key_: AWS access key (Optional)
        :param secret_key_: AWS secret key (Optional)
        :param region_: AWS region (Default us-east-2)
        :param topic_arn_: Full Topic ARN, if not supplying topic_name_
            (ex. arn:aws:sns:us-east-2:{account_id}:prod-glue-tnt-alerts)
        :return: Dictionary of subscribe results
        """
        sns = boto3.client('sns', region_name=region_,
                           aws_access_key_id=access_key_,
                           aws_secret_access_key=secret_key_)

        if topic_arn_:
            topic_arn = topic_arn_
        elif topic_name_:
            account_id = AWSHelpers.get_account_id()
            topic_arn = 'arn:aws:sns:{}:{}:{}'.format(region_, account_id,
                                                      topic_name_)
        else:
            raise Exception("Must provide a topic name or topic ARN.")

        response = sns.subscribe(
            TopicArn=topic_arn,
            Protocol=protocol_,
            Endpoint=endpoint_
        )

        return response

    @staticmethod
    def sns_publish(topic_name_, message_=None, subject_=None, topic_arn_=None,
                    access_key_=None, secret_key_=None, region_='us-east-2'):
        """
        Simple method for sending a message via SNS.
        Note: a topic needs to have already been set up to utilize
            this function.

        :param topic_name_: Topic name (ex. 'prod-glue-tnt-events')
        :param subject_: Subject for e-mail. If None, will default
            to 'AWS Notification for Data Engineering'.
        :param message_: Message to send to topic. Will be converted to string.
        :param access_key_: AWS access key (Optional)
        :param secret_key_: AWS secret key (Optional)
        :param region_: AWS region. 'us-east-2' default if None.
        :param topic_arn_: Full Topic ARN, if not supplying topic_name_
            (ex. arn:aws:sns:us-east-2:{account_id}:prod-glue-tnt-alerts)
        :return: Logging to console of message sent.
        """

        sns = boto3.client('sns', region_name=region_,
                           aws_access_key_id=access_key_,
                           aws_secret_access_key=secret_key_)

        if topic_arn_:
            topic_arn = topic_arn_
        elif topic_name_:
            account_id = AWSHelpers.get_account_id()
            topic_arn = 'arn:aws:sns:{}:{}:{}'.format(region_, account_id,
                                                      topic_name_)
        else:
            raise Exception("Must provide a topic name or topic ARN.")

        subject = subject_ if subject_ else "AWS Notification for " \
                                            "Data Engineering"

        response = sns.publish(
            TopicArn=topic_arn,
            Subject=str(subject),
            Message=str(message_)
        )

        print(response)
        logging.info("Sent the following message to SNS topic:\n{}"
                     .format(message_))

    @staticmethod
    def cloudwatch_put_rule(event_name_, event_pattern_, event_desc_: str = '',
                            state_="ENABLED", access_key_=None,
                            secret_key_=None, region_='us-east-2'):
        """
        Creates a Cloudwatch Event rule.

        :param event_name_: Name for Cloudwatch Event
        :param event_desc_: Description for Cloudwatch Event
        :param event_pattern_: Pattern for event (Represented as JSON). More info at:
            https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/CloudWatchEventsandEventPatterns.html
        :param state_: State of Event (Options: 'ENABLED'/'DISABLED',
            Default = 'ENABLED')
        :param access_key_: AWS access key (Optional)
        :param secret_key_: AWS secret key (Optional)
        :param region_: AWS region (Default = 'us-east-2')
        :return: Dictionary of rule creation details
        """

        events = boto3.client('events', region_name=region_,
                              aws_access_key_id=access_key_,
                              aws_secret_access_key=secret_key_)

        response = events.put_rule(
            Name=event_name_,
            Description=event_desc_,
            EventPattern=event_pattern_,
            State=state_
        )

        return response

    @staticmethod
    def cloudwatch_put_target(event_name_, target_name_, target_id_=None,
                              input_map_=None, input_template_=None,
                              access_key_=None, secret_key_=None,
                              region_='us-east-2'):
        """
        Assigns a target for a Cloudwatch Event rule.

        Note: We currently use SNS as a target with e-mail subscription. This
        function is setup for that purpose, but, can be modified later on for
        other targets/subs.

        :param event_name_: Name of Cloudwatch Event we want target assigned to
        :param target_name_: Name of target (For SNS, topic name. Will be
            converted to ARN).
        :param target_id_: Name of ID for target (For SNS, leave as None.
            Will be converted to topic name from target_name_ value).
        :param input_map_: JSON string of key/value pairs
            from event details (Optional)
        :param input_template_: String of message layout (Optional).
        :param access_key_: AWS access key (Optional)
        :param secret_key_: AWS secret key (Optional)
        :param region_: AWS region (Default = 'us-east-2')
        :return: Dictionary of target assignment details
        """

        events = boto3.client('events', region_name=region_,
                              aws_access_key_id=access_key_,
                              aws_secret_access_key=secret_key_)

        account_id = AWSHelpers.get_account_id()
        topic_arn = 'arn:aws:sns:{}:{}:{}'.format(region_, account_id,
                                                  target_name_)

        response = events.put_targets(
            Rule=event_name_,
            Targets=[
                {
                    'Arn': topic_arn,
                    'Id': target_name_,
                    'InputTransformer': {
                        'InputPathsMap': input_map_,
                        'InputTemplate': input_template_
                    }
                    }
                ]
        )

        return response

    @staticmethod
    def create_ses_template(template_name_,subject_,body_=None
                ,overwrite_=False,src_="string",html_=True,s3_params_=None,filepath_=None
                ,access_key_=None, secret_key_=None
                ,region_='us-east-1',charset_="utf-8"):
        '''
        @param template_name_: name of SES template (mandatory)
        @param subject_: email subject portion of the template (mandatory)
        @param body_: email body portion of the template (default None), required parameter when src_="string"
        @param overwrite_: flag which indicates whether existing template with the same name should be overwritten (default False)
        @param src_: indicates the source of template data
                    Accepts:
                        s3 - template data is provided in s3, S3_params_ must be provided
                        file - template data is provided in local file, filepath_ must be provided
                        string (Default) - template data is passed in string, body_ must be provided
        @param html_: specifies whether the email body is html or text (default True)
        @param s3_params_: required parameter when src_="s3"
        @param filepath_: required parameter when src_="file"
        @return: statusCode
        '''
        if src_.lower() not in ["s3","file","string"]:
            logging.error('Parameter "src_" must be one if these "s3","file","string"')
            raise("")

        if subject_ is None:
            logging.error('Parameter subject_ must be provided')
            raise("")
        
        if src_.lower()=="s3":
            if s3_params_ is None:
                raise('Must supply S3 parameters as dict in s3_params_')
            if not isinstance(s3_params_,dict):
                raise('Parameter "s3_params_" must be of type Dict')
            if "bucket_name" not in s3_params_ or "filename" not in s3_params_:
                raise('Both "bucket_name" and "filename" must be provided when S3 selected for src_')

            filename=s3_params_["filename"]
            bucket_name=s3_params_["bucket_name"]
            file=filename.split(".")[0]
            file_type=filename.split(".")[1]

            prefix="" if "prefix" not in s3_params_ else s3_params_["prefix"]
            df_schema="" if "df_schema" not in s3_params_ else s3_params_["df_schema"]
            check_headers= False if "check_headers" not in s3_params_ else s3_params_["check_headers"]
            compression_type=None if "compression_type" not in s3_params_ else s3_params_["compression_type"]
            decode='utf-8' if "decode" not in s3_params_ else s3_params_["decode"]

            my_file = S3Data(bucket_name_=bucket_name,
                            prefix_=prefix,
                            file_=file,
                            df_schema_=df_schema,
                            check_headers_=check_headers,
                            file_type_=file_type,
                            compression_type_=compression_type,
                            decode_=decode
                            )

            my_file.load_data(skip_schema=True)
            body=my_file.lines
        elif src_.lower()=="file":
            if filepath_ is None:
                raise('Must supply filepath to local file with template in "filepath_" when "file" selected for src_')
            with open(filepath_) as f:
                body=f.read()
        elif src_.lower()=="string":
            if body_ is None:
                raise('Parameter body_ must be provided when "string" selected for src_')
            body=body_
        
        body_type="HtmlPart" if html_ is True else "TextPart"
        
        ses_client = boto3.client('ses',region_name=region_,
                              aws_access_key_id=access_key_,
                              aws_secret_access_key=secret_key_)
        if overwrite_==True:
            response=AWSHelpers.delete_ses_template(template_name_)
        try:
            response = ses_client.create_template(
                Template={
                    'TemplateName': template_name_,
                    'SubjectPart': subject_,
                    body_type: body
                }
            )
        except ClientError as e:
            logging.error(e.response["Error"]["Code"]) #'AlreadyExists'
            raise ValueError(e.response["Error"]["Code"])
                
        return response["ResponseMetadata"]["HTTPStatusCode"]

    @staticmethod
    def delete_ses_template(template_name_
                ,access_key_=None, secret_key_=None
                ,region_='us-east-1',charset_="utf-8"):
        ses_client = boto3.client('ses',region_name=region_,
                              aws_access_key_id=access_key_,
                              aws_secret_access_key=secret_key_)
        try:
            response=ses_client.delete_template(TemplateName=template_name_)
        except ClientError as e:
            logging.error(e)

    @staticmethod
    def send_ses_email(recipients_,subject_,content_,html_=True
                ,sender="sdcde@smiledirectclub.com"
                ,access_key_=None, secret_key_=None
                ,region_='us-east-1',charset_="utf-8"):
        '''
        @param recipients_: recipients to receive the same copy of the email
        @param subject_: subject of the email
        @param content_: body of the email
        @param html_: indicates whether the email body is html or text (default True)
        @param sender: "FROM" address on the email to send. This email has to be verified by AWS first
        @return: statusCode
        '''
        if not isinstance(recipients_,list):
            raise ValueError(f'Parameter "recipients_" must be a list')
        subject={
            'Charset': charset_,
            'Data': subject_,
        }
        if html_:
            body_type="Html"
        else:
            body_type="Text"
        body={
            body_type:{
                'Charset': charset_,
                'Data': (content_),
            }
        }
        
        ses_client = boto3.client('ses',region_name=region_,
                              aws_access_key_id=access_key_,
                              aws_secret_access_key=secret_key_)
        try:
            response = ses_client.send_email(
                Destination={
                    'ToAddresses': recipients_,
                },
                Message={
                    'Body': body,
                    'Subject': subject,
                },
                Source=sender,
            )
        except ClientError as e:
            logging.error(e.response['Error']['Message'])
            raise BaseException(e.response['Error']['Message'])
        else:
            print(response['ResponseMetadata']['HTTPStatusCode'])
            return response['ResponseMetadata']['HTTPStatusCode']

    @staticmethod
    def send_templated_ses_email(recipients_,template_name_,template_data_
                ,sender="sdcde@smiledirectclub.com"
                ,access_key_=None, secret_key_=None
                ,region_='us-east-1',charset_="utf-8"):
        '''
        @param recipients_: list of emails to receive templated email
        @param template_name_: template to apply (has to be previously uploaded)
        @param template_data_: dynamic data to fit into the template
        @param sender: "FROM" address on the email to send. This email has to be verified by AWS first
        @return: statusCode
        '''
        if not isinstance(recipients_,list):
            raise ValueError(f'Parameter "recipients_" must be a list')
        if not isinstance(template_data_,str):
            raise ValueError(f'Parameter "template_data_" must be a str')
        
        ses_client = boto3.client('ses',region_name=region_,
                              aws_access_key_id=access_key_,
                              aws_secret_access_key=secret_key_)
        try:
            ####### ATTN! Every recipient will receive identical email. If you need to send customized emails use send_bulk_templated_email instead ####
            response = ses_client.send_templated_email(
                Source=sender,
                Destination={
                    'ToAddresses': recipients_,
                },
                Template=template_name_,
                TemplateData=template_data_
            )
        except ClientError as e:
            logging.error(e.response['Error']['Message'])
            raise BaseException(e.response['Error']['Message'])
        except:
            raise BaseException("It's dead, Jimmy.")
        else:
            return response['ResponseMetadata']['HTTPStatusCode']
