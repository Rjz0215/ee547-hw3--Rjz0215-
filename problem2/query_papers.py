#!/usr/bin/env python3
import sys, os, json, argparse, time
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key

AUTHOR_INDEX = "AuthorIndex"
PAPER_INDEX  = "PaperIdIndex"
KEYWORD_INDEX= "KeywordIndex"

def dynamo(table_name, region):
    session = boto3.Session(region_name=region)
    return session.resource("dynamodb").Table(table_name)

def out(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2))

def query_recent_in_category(table_name, category, limit, region):
    t0 = time.time()
    table = dynamo(table_name, region)
    resp = table.query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
        ScanIndexForward=False,
        Limit=limit
    )
    items = resp.get("Items", [])
    return {
        "query_type": "recent_in_category",
        "parameters": {"category": category, "limit": limit},
        "results": items,
        "count": len(items),
        "execution_time_ms": int((time.time()-t0)*1000)
    }

def query_papers_by_author(table_name, author_name, region):
    t0 = time.time()
    table = dynamo(table_name, region)
    resp = table.query(
        IndexName=AUTHOR_INDEX,
        KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
    )
    items = resp.get("Items", [])
    return {
        "query_type": "papers_by_author",
        "parameters": {"author_name": author_name},
        "results": items,
        "count": len(items),
        "execution_time_ms": int((time.time()-t0)*1000)
    }

def get_paper_by_id(table_name, arxiv_id, region):
    t0 = time.time()
    table = dynamo(table_name, region)
    resp = table.query(
        IndexName=PAPER_INDEX,
        KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}')
    )
    items = resp.get("Items", [])
    item = items[0] if items else None
    return {
        "query_type": "get_paper_by_id",
        "parameters": {"arxiv_id": arxiv_id},
        "results": [item] if item else [],
        "count": 1 if item else 0,
        "execution_time_ms": int((time.time()-t0)*1000)
    }

def query_papers_in_date_range(table_name, category, start_date, end_date, region):
    t0 = time.time()
    table = dynamo(table_name, region)
    resp = table.query(
        KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}') &
                               Key('SK').between(f'{start_date}#', f'{end_date}#zzzzzzz')
    )
    items = resp.get("Items", [])
    return {
        "query_type": "papers_in_date_range",
        "parameters": {"category": category, "start_date": start_date, "end_date": end_date},
        "results": items,
        "count": len(items),
        "execution_time_ms": int((time.time()-t0)*1000)
    }

def query_papers_by_keyword(table_name, keyword, limit, region):
    t0 = time.time()
    table = dynamo(table_name, region)
    resp = table.query(
        IndexName=KEYWORD_INDEX,
        KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword.lower()}'),
        ScanIndexForward=False,
        Limit=limit
    )
    items = resp.get("Items", [])
    return {
        "query_type": "papers_by_keyword",
        "parameters": {"keyword": keyword, "limit": limit},
        "results": items,
        "count": len(items),
        "execution_time_ms": int((time.time()-t0)*1000)
    }

def parse_args():
    ap = argparse.ArgumentParser(description="Query ArXiv papers in DynamoDB")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap.add_argument("--table", default=os.environ.get("ARXIV_TABLE","arxiv-papers"))
    ap.add_argument("--region", default=os.environ.get("AWS_REGION","us-east-1"))

    p1 = sub.add_parser("recent")
    p1.add_argument("category")
    p1.add_argument("--limit", type=int, default=20)

    p2 = sub.add_parser("author")
    p2.add_argument("author_name")

    p3 = sub.add_parser("get")
    p3.add_argument("arxiv_id")

    p4 = sub.add_parser("daterange")
    p4.add_argument("category")
    p4.add_argument("start_date")
    p4.add_argument("end_date")

    p5 = sub.add_parser("keyword")
    p5.add_argument("keyword")
    p5.add_argument("--limit", type=int, default=20)

    return ap.parse_args()

def main():
    args = parse_args()
    if args.cmd == "recent":
        out(query_recent_in_category(args.table, args.category, args.limit, args.region))
    elif args.cmd == "author":
        out(query_papers_by_author(args.table, args.author_name, args.region))
    elif args.cmd == "get":
        out(get_paper_by_id(args.table, args.arxiv_id, args.region))
    elif args.cmd == "daterange":
        out(query_papers_in_date_range(args.table, args.category, args.start_date, args.end_date, args.region))
    elif args.cmd == "keyword":
        out(query_papers_by_keyword(args.table, args.keyword, args.limit, args.region))

if __name__ == "__main__":
    main()
