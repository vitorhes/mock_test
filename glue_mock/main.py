import boto3
import datetime
import random
from moto import mock_glue,mock_athena


class MockTableinGlue:

    def __init__(self) -> None:
        pass

    @mock_glue
    def _create_mock_table(self):
        # Start the mock Glue service
        with mock_glue():
            # Create a Glue client
            glue_client = boto3.client('glue', region_name='us-east-1')

            # Simulate creating a database
            database_name = 'test_database'
        
            glue_client.create_database(DatabaseInput={'Name': database_name})

            # Simulate creating a table with multiple partitions
            table_name = 'test_table'
            input_location = 's3://your-bucket/path/to/data/'

            glue_client.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': {
                        'Location': input_location,
                        'Columns': [
                            {'Name': 'column1', 'Type': 'string'},
                            {'Name': 'column2', 'Type': 'int'},
                        ],
                    },
                    'PartitionKeys': [
                        {'Name': 'year', 'Type': 'string'},
                        {'Name': 'month', 'Type': 'string'},
                        {'Name': 'day', 'Type': 'string'},
                    ],
                }
            )

            # Simulate creating multiple partitions with different CreationTime
            today = datetime.datetime.utcnow().date()
            for i in range(10):
                partition_date = today - datetime.timedelta(days=i)
                partition_input = {
                    'Values': [str(partition_date.year), str(partition_date.month), str(partition_date.day)],
                    'StorageDescriptor': {
                        'Location': f'{input_location}year={partition_date.year}/month={partition_date.month}/day={partition_date.day}/',
                        'Columns': [
                            {'Name': 'column1', 'Type': 'string'},
                            {'Name': 'column2', 'Type': 'int'},
                        ],
                    },
                }
                glue_client.create_partition(DatabaseName=database_name, TableName=table_name, PartitionInput=partition_input)
        return database_name, table_name
    
    @mock_glue
    def get_partitions(self):
        with mock_glue():
            database_name, table_name = self._create_mock_table()
            # Create a Glue client
            glue_client = boto3.client('glue', region_name='us-east-1')

            # Simulate getting partition metadata, ordering by CreationTime
            response = glue_client.get_partitions(DatabaseName=database_name, TableName=table_name)

            # Order partitions by CreationTime
            partitions_metadata = sorted(response['Partitions'], key=lambda x: x['CreationTime'])

            # Print the ordered partitions metadata
            print("Ordered Partitions Metadata:")
            for partition in partitions_metadata:
                print(partition)
    

if __name__ == "__main__":
    print("helo")
    mocker = MockTableinGlue()

    mocker.get_partitions()
