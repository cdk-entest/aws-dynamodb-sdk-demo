# haimtran 01 DEC 2022
# dynamodb pagination
import boto3

TABLE_NAME = "Restaurants"

client = boto3.client("dynamodb")

def get_item_by_key():
    """
    get item by primary key (partition key and sort key)
    """
    resp = client.get_item(
        TableName=TABLE_NAME,
        Key={
            'PK': {
                'S': 'USER#christopherperkins',
            },
            'SK': {
                'S': 'REST#AnyCompany BBQ'
            }
        }
    )
    print(resp)


def query_items_with_next_token(next_token):
    """
    query items given the current pointer or next token
    """
    resp = client.query(
        TableName=TABLE_NAME,
        Select='ALL_ATTRIBUTES',
        Limit=1,
        KeyConditions={
            'PK': {
                'AttributeValueList': [
                    {
                        'S': 'USER#christopherperkins'
                    }
                ],
                'ComparisonOperator': 'EQ'
            }
        },
        ExclusiveStartKey=next_token,
        ScanIndexForward=False
    )
    #
    print(resp)
    # check if there are more item to fetch
    if "LastEvaluatedKey" in resp:
        next_token = resp["LastEvaluatedKey"]
    else:
        next_token = None
    # return 
    return next_token

def query_items():
    """
    query and check the LastEvaluatedKey which indicate the current pointer
    if it is NULL, it means no more item to fetch
    """
    # do the first query
    resp = client.query(
        TableName=TABLE_NAME,
        Select='ALL_ATTRIBUTES',
        Limit=1,
        KeyConditions={
            'PK': {
                'AttributeValueList': [
                    {
                        'S': 'USER#christopherperkins'
                    }
                ],
                'ComparisonOperator': 'EQ'
            }
        },
        ScanIndexForward=False
    )
    # check next token
    print("\n=================================\n")
    next_token = resp["LastEvaluatedKey"]
    print(next_token)
    # print("\n=================================\n")
    print(resp)
    while next_token is not None:
        print("\n=================================\n")
        print(next_token)
        print("\n=================================\n")
        # check the last EvaluatedKey
        next_token = query_items_with_next_token(next_token=next_token)
    # return
    return None


if __name__=="__main__":
    query_items()