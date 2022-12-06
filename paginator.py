# haimtran 05 DEC 2022
# dynamodb pagination demo
import boto3
import random
from datetime import datetime
import json

# table name
TABLE_NAME = "Game"

# game titles
game_titles = ["Galaxy Invader", "Meteor Blasters"]

# user or player id
user_ids = [str(x) for x in range(100, 121)]


def create_table(table_name):
    """
    create a table
    """
    # client
    client = boto3.client("dynamodb")
    # create a table
    res = client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "UserId", "AttributeType": "S"},
            {"AttributeName": "CreatedTime", "AttributeType": "N"},
        ],
        # Keyschema and Attribute should be the same
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


def write_table() -> None:
    """
    write data to table
    """
    # table
    table = boto3.resource("dynamodb").Table("Game")

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
                    "CreatedTime": int(datetime.now().timestamp()),
                }
            )
            print(res)


def test_paginator(table_name: str, index_name: str) -> None:
    """
    paginagtor
    """
    # client
    client = boto3.client("dynamodb")
    # paginator
    paginator = client.get_paginator("scan")
    # iterator
    for page in paginator.paginate(
        TableName=table_name, PaginationConfig={"PageSize": 2}
    ):
        print(json.dumps(page, indent=2, default=str))
        # print(page["LastEvaluatedKey"])
        # print(page)
        if "LastEvaluatedKey" in page:
            pass 
        else:
            print("no more data")
        print("======================================================")


if __name__ == "__main__":
    # create_table(table_name=TABLE_NAME)
    # write_table()
    test_paginator(table_name=TABLE_NAME, index_name="")
