# DyanmoDB with Global Secondary Index

![Screen Shot 2022-05-19 at 01 12 00](https://user-images.githubusercontent.com/20411077/169115332-cb521eee-59dd-4900-ae18-c532748c65f0.png)

## Python boto3 SDK

boto3 resource and table

```python
# create ddb client
ddb = boto3.resource('dynamodb')

# table
table = ddb.Table("Game")
```

create a table

```python
def create_table() -> None:
    """
    create a table
    """
    # db client, optional region specified here
    db_client = boto3.client('dynamodb')
    # create a table
    res = db_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {
                'AttributeName': 'UserId',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'CreatedTime',
                'AttributeType': 'N'
            },
        ],
        # KeySchema and Attribute should be the same
        KeySchema=[
            {
                'AttributeName': 'UserId',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'CreatedTime',
                'KeyType': 'RANGE'
            },
        ],
        # PAY_PER_REQUEST when load is unpredictable
        # PROVISIONED when load is predictable
        BillingMode="PAY_PER_REQUEST"
    )
```

write to a table

```python
def write_table() -> None:
    """
    write to game table
    """

    # create a new item
    for game_title in game_titles:
        for user_id in user_ids:
            res = table.put_item(
                Item={
                    'UserId': user_id,
                    'GameTitle': game_title,
                    'Score': random.randint(1000, 6000),
                    'Wins': random.randint(0, 100),
                    'Losses': random.randint(5, 50),
                    'CreatedTime': int(datetime.datetime.now().timestamp() * 1000)
                }
            )
            print(res)

    # return
    return 1

```

query by id

```python
def query_by_user_id(user_id: str) -> None:
    """
        query items in a table
    """

    # query by user id
    res = table.query(
        KeyConditionExpression=Key('UserId').eq(user_id),
        ScanIndexForward=False,
    )
    # print result
    for item in res['Items']:
        print(item)
```

create a global secondary index

```python
def create_global_secondary_index(index_name: str) -> None:
    """
    update table with global secondary index
    """
    #
    client = boto3.client('dynamodb')
    #
    res = client.update_table(
        TableName="Game",
        AttributeDefinitions=[
            {
                "AttributeName": "UserId",
                "AttributeType": "S"
            },
            {
                "AttributeName": "CreatedTime",
                "AttributeType": "N"
            },
            {
                "AttributeName": "GameTitle",
                "AttributeType": "S"
            },
            {
                "AttributeName": "Score",
                "AttributeType": "N"
            },
        ],
        GlobalSecondaryIndexUpdates=[
            {
                "Create": {
                    "IndexName": index_name,
                    "KeySchema": [
                        {
                            "AttributeName": "GameTitle",
                            "KeyType": "HASH"
                        },
                        {
                            "AttributeName": "Score",
                            "KeyType": "RANGE"
                        }
                    ],
                    "Projection": {
                        "ProjectionType": "KEYS_ONLY"
                    },
                    # PAY_PER_REQUEST no need provisioned
                    # "ProvisionedThroughput": {
                    #     "ReadCapacityUnits": 1,
                    #     "WriteCapacityUnits": 1,
                    # }
                }
            }
        ]
    )
```

query by index

```python
def query_index(index_name) -> None:
    """
    query with secondary index
    """
    res = table.query(
        IndexName=index_name,
        KeyConditionExpression=Key('GameTitle').eq('Galaxy Invaders'),
        ScanIndexForward=False,
        Limit=10,
    )
    items = res['Items']
    for item in items:
        print(item)
```

delete table

```python
def delete_table() -> None:
    """
    delete a table
    """
    res = table.delete()
```

## Reference

- [dynamodb boto3](https://aws.amazon.com/getting-started/projects/create-manage-nonrelational-database-dynamodb/4/)
- [game leaderboard](https://aws.amazon.com/blogs/database/amazon-dynamodb-gaming-use-cases-and-design-patterns/)
