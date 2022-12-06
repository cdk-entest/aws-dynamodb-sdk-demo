"""
haimtran dynamodb demo
1. create a game table
2. query table
3. global secondary index to get topscore of a game
"""

import datetime
import random
import boto3
from boto3.dynamodb.conditions import Key

# table name
table_name = "Game"

# game titles
game_titles = [
    "Galaxy Invaders",
    "Meteor Blasters",
]

# user or player id
user_ids = [str(x) for x in range(100, 121)]

# create ddb client
ddb = boto3.resource("dynamodb")

# table
table = ddb.Table("Game")


# def clean_table() -> None:
#     """
#     clean a table
#     """

#     # scan all items
#     res = table.scan(Limit=100)
#     # loop through each item and delete
#     items = res["Items"]
#     for item in items:
#         # print(int(item['CreatedTime']))
#         res = table.delete_item(
#             Key={"UserId": str(item["UserId"]), "CreatedTime": int(item["CreatedTime"])}
#         )
#         print(res)


def write_table() -> None:
    """
    write to game table
    """

    # create a new item
    for game_title in game_titles:
        for user_id in user_ids:
            res = table.put_item(
                Item={
                    "UserId": user_id,
                    "GameTitle": game_title,
                    "Score": random.randint(1000, 6000),
                    "Wins": random.randint(0, 100),
                    "Losses": random.randint(5, 50),
                    "CreatedTime": int(datetime.datetime.now().timestamp() * 1000),
                }
            )
            print(res)

    # return
    return 1


def query_by_user_id(user_id: str) -> None:
    """
    query items in a table
    """

    # query by user id
    res = table.query(
        KeyConditionExpression=Key("UserId").eq(user_id),
        ScanIndexForward=False,
    )
    # print result
    for item in res["Items"]:
        print(item)


def create_global_secondary_index(index_name: str) -> None:
    """
    update table with global secondary index
    """
    #
    client = boto3.client("dynamodb")
    #
    res = client.update_table(
        TableName="Game",
        AttributeDefinitions=[
            {"AttributeName": "UserId", "AttributeType": "S"},
            {"AttributeName": "CreatedTime", "AttributeType": "N"},
            {"AttributeName": "GameTitle", "AttributeType": "S"},
            {"AttributeName": "Score", "AttributeType": "N"},
        ],
        GlobalSecondaryIndexUpdates=[
            {
                "Create": {
                    "IndexName": index_name,
                    "KeySchema": [
                        {"AttributeName": "GameTitle", "KeyType": "HASH"},
                        {"AttributeName": "Score", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "KEYS_ONLY"},
                    # PAY_PER_REQUEST no need provisioned
                    # "ProvisionedThroughput": {
                    #     "ReadCapacityUnits": 1,
                    #     "WriteCapacityUnits": 1,
                    # },
                }
            }
        ],
    )
    print(res)


def query_index(index_name) -> None:
    """
    query with secondary index
    """
    res = table.query(
        IndexName=index_name,
        KeyConditionExpression=Key("GameTitle").eq("Galaxy Invaders"),
        ScanIndexForward=False,
        Limit=10,
    )
    items = res["Items"]
    for item in items:
        print(item)


def delete_table() -> None:
    """
    delete a table
    """
    res = table.delete()
    print(res)


def create_table() -> None:
    """
    create a table
    """
    # db client, optional region specified here
    db_client = boto3.client("dynamodb")
    # create a table
    res = db_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "UserId", "AttributeType": "S"},
            {"AttributeName": "CreatedTime", "AttributeType": "N"},
        ],
        # KeySchema and Attribute should be the same
        KeySchema=[
            {"AttributeName": "UserId", "KeyType": "HASH"},
            {"AttributeName": "CreatedTime", "KeyType": "RANGE"},
        ],
        # PAY_PER_REQUEST when load is unpredictable
        # PROVISIONED when load is predictable
        BillingMode="PAY_PER_REQUEST",
    )
    #
    print(res)


# ====================================== TEST ===================================
if __name__ == "__main__":
    # create_table()
    # delete_table()
    # clean_table()
    write_table()
    # query_by_user_id(user_id="102")
    # clean_table()
    # create_global_secondary_index(index_name="GameTitleScoreIndex")
    # query_index(index_name="GameTitle-Score-index")
