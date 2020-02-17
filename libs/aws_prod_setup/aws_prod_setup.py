
from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers


# Create SNS topic for Glue Alerts
AWSHelpers.sns_create_topic('prod-de-glue-alerts')


# Subscribe DE DL to SNS topic
AWSHelpers.sns_subscribe(topic_name_='prod-de-glue-alerts',
                         protocol_='email',
                         endpoint_='sdcde@smiledirectclub.com')


# Create Cloudwatch Event for Failed Glue Jobs
EVENT_PATTERN = """
{
  "source": [
    "aws.glue"
  ],
  "detail-type": [
    "Glue Job State Change"
  ],
  "detail": {
    "state": [
      "FAILED"
    ],
    "jobName": [
      "Fantasia-Snowflake",
      "Fedex-to-Snowflake",
      "TNT-to-Snowflake"
    ]
  }
}
"""

AWSHelpers.cloudwatch_put_rule(event_name_="prod-de-glue-failed-jobs",
                               event_desc_="Data Engineering failed Glue jobs",
                               event_pattern_=EVENT_PATTERN)


# Assign SNS topic to Cloudwatch Event
INPUT_MAP =  {
    "severity":"$.detail.severity",
    "jobrunid":"$.detail.jobRunId",
    "state":"$.detail.state",
    "time":"$.time",
    "message":"$.detail.message",
    "jobname":"$.detail.jobName"
}


INPUT_TEMPLATE = """
"The <jobname> Glue Job has <state>."
"Time: <time>"
"Severity: <severity>"
"Job Run Id: <jobrunid>"
"Message: <message>"
"""

AWSHelpers.cloudwatch_put_target(event_name_="prod-de-glue-failed-jobs",
                                 target_name_="prod-de-glue-alerts",
                                 input_map_=INPUT_MAP,
                                 input_template_=INPUT_TEMPLATE)

