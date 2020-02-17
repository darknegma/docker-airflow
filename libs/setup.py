import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sdc_etl_libs",
    version="0.0.61",
    author="Matthew Garcia",
    author_email="matthew.garcia@smiledirectclub.com",
    description="This is a package of all our etl libs used here at SDC",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CamelotVG/data-engineering",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires = [
        'pandas',
        'boto3',
        'numpy',
        'sqlalchemy',
        'snowflake',
        'pyodbc',
        'Salesforce-FuelSDK',
        'zeep',
        'backoff', 
        'gnupg',
        'python-gnupg',
        'google-api-python-client'
        ,'psycopg2'
    ],
    classifiers=[
        "Programming Language :: Python :: 3"
    ],
)
