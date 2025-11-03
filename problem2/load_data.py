#!/usr/bin/env python3
import sys, os, json, re, argparse
from datetime import datetime
from collections import Counter, defaultdict
import boto3
from botocore.exceptions import ClientError

STOPWORDS = {
    'the','a','an','and','or','but','in','on','at','to','for','of','with','by','from','up',
    'about','into','through','during','is','are','was','were','be','been','being','have','has',
    'had','do','does','did','will','would','could','should','may','might','can','this','that',
    'these','those','we','our','use','using','based','approach','method','paper','propose',
    'proposed','show'
}

TABLE_BILLING = "PAY_PER_REQUEST"
AUTHOR_INDEX = "AuthorIndex"
PAPER_INDEX  = "PaperIdIndex"
KEYWORD_INDEX= "KeywordIndex"

def parse_args():
    ap = argparse.ArgumentParser(description="Load ArXiv papers into DynamoDB (denormalized)")
    ap.add_argument("papers_json_path")
    ap.add_argument("table_name")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION","us-east-1"))
    return ap.parse_args()

def ensure_table(dynamodb, table_name):
    ddb = dynamodb
    existing = {t.name for t in ddb.tables.all()}
    if table_name in existing:
        return ddb.Table(table_name)

    print(f"Creating DynamoDB table: {table_name}")
    table = ddb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName":"PK", "KeyType":"HASH"},
            {"AttributeName":"SK", "KeyType":"RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName":"PK", "AttributeType":"S"},
            {"AttributeName":"SK", "AttributeType":"S"},
            {"AttributeName":"GSI1PK", "AttributeType":"S"},
            {"AttributeName":"GSI1SK", "AttributeType":"S"},
            {"AttributeName":"GSI2PK", "AttributeType":"S"},
            {"AttributeName":"GSI2SK", "AttributeType":"S"},
            {"AttributeName":"GSI3PK", "AttributeType":"S"},
            {"AttributeName":"GSI3SK", "AttributeType":"S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": AUTHOR_INDEX,
                "KeySchema": [
                    {"AttributeName":"GSI1PK","KeyType":"HASH"},
                    {"AttributeName":"GSI1SK","KeyType":"RANGE"},
                ],
                "Projection": {"ProjectionType":"ALL"},
            },
            {
                "IndexName": PAPER_INDEX,
                "KeySchema": [
                    {"AttributeName":"GSI2PK","KeyType":"HASH"},
                    {"AttributeName":"GSI2SK","KeyType":"RANGE"},
                ],
                "Projection": {"ProjectionType":"ALL"},
            },
            {
                "IndexName": KEYWORD_INDEX,
                "KeySchema": [
                    {"AttributeName":"GSI3PK","KeyType":"HASH"},
                    {"AttributeName":"GSI3SK","KeyType":"RANGE"},
                ],
                "Projection": {"ProjectionType":"ALL"},
            },
        ],
        BillingMode=TABLE_BILLING,
    )
    print("Waiting for table to be active...")
    table.wait_until_exists()
    return table

def tokenize(text):
    words = re.findall(r"[A-Za-z][A-Za-z0-9\-]*", text or "")
    return [w.lower() for w in words]

def extract_keywords(abstract, topk=10):
    tokens = tokenize(abstract)
    filtered = [t for t in tokens if t not in STOPWORDS and len(t) >= 3]
    counts = Counter(filtered)
    return [w for w,_ in counts.most_common(topk)]

def iso_to_date(iso_str):
    # expected ISO time e.g. "2023-01-15T10:30:00Z"
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z","+00:00"))
    except ValueError:
        # fallback: try first 10 chars as date
        return iso_str[:10]
    return dt.strftime("%Y-%m-%d")

def safe_str(x):
    return "" if x is None else str(x)

def load_papers(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Accept either list or dict with "papers"
    if isinstance(data, dict) and "papers" in data:
        data = data["papers"]
    return data

def main():
    args = parse_args()
    session = boto3.Session(region_name=args.region)
    dynamodb = session.resource("dynamodb")
    table = ensure_table(dynamodb, args.table_name)

    print(f"Loading papers from {args.papers_json_path}...")
    papers = load_papers(args.papers_json_path)

    print("Extracting keywords from abstracts...")
    total_papers = 0
    stats = defaultdict(int)

    with table.batch_writer(overwrite_by_pkeys=["PK","SK"]) as bw:
        for p in papers:
            arxiv_id = safe_str(p.get("arxiv_id") or p.get("id"))
            title = safe_str(p.get("title"))
            authors = p.get("authors") or []
            abstract = safe_str(p.get("abstract"))
            categories = p.get("categories") or []
            published_iso = safe_str(p.get("published") or p.get("updated") or p.get("date"))
            if not arxiv_id or not title or not published_iso:
                continue

            date_str = iso_to_date(published_iso)
            keywords = extract_keywords(abstract, topk=10)

            core = {
                "arxiv_id": arxiv_id,
                "title": title,
                "authors": authors,
                "abstract": abstract,
                "categories": categories,
                "keywords": keywords,
                "published": published_iso,
            }

            # Paper ID item (for direct lookup)
            paper_item = {
                "PK": f"PAPER#{arxiv_id}",
                "SK": "A",
                "GSI2PK": f"PAPER#{arxiv_id}",
                "GSI2SK": date_str,
                **core,
            }
            bw.put_item(Item=paper_item)
            stats["paper_id_items"] += 1

            # Category items (one per category)
            for cat in categories:
                cat_item = {
                    "PK": f"CATEGORY#{cat}",
                    "SK": f"{date_str}#{arxiv_id}",
                    **core,
                }
                bw.put_item(Item=cat_item)
                stats["category_items"] += 1

            # Author items (one per author)
            for author in authors:
                a_item = {
                    "PK": f"AUTHOR#{author}",
                    "SK": f"{date_str}#{arxiv_id}",
                    "GSI1PK": f"AUTHOR#{author}",
                    "GSI1SK": f"{date_str}#{arxiv_id}",
                    **core,
                }
                bw.put_item(Item=a_item)
                stats["author_items"] += 1

            # Keyword items (one per keyword)
            for kw in keywords:
                k_item = {
                    "PK": f"KEYWORD#{kw}",
                    "SK": f"{date_str}#{arxiv_id}",
                    "GSI3PK": f"KEYWORD#{kw}",
                    "GSI3SK": f"{date_str}#{arxiv_id}",
                    **core,
                }
                bw.put_item(Item=k_item)
                stats["keyword_items"] += 1

            total_papers += 1

    total_items = stats["paper_id_items"] + stats["category_items"] + stats["author_items"] + stats["keyword_items"]
    factor = (total_items / total_papers) if total_papers else 0.0

    print(f"Loaded {total_papers} papers")
    print(f"Created {total_items} DynamoDB items (denormalized)")
    print(f"Denormalization factor: {factor:.1f}x\n")
    print("Storage breakdown:")
    print(f"  - Category items: {stats['category_items']} ({(stats['category_items']/total_papers):.1f} per paper avg)")
    print(f"  - Author items:   {stats['author_items']} ({(stats['author_items']/total_papers):.1f} per paper avg)")
    print(f"  - Keyword items:  {stats['keyword_items']} ({(stats['keyword_items']/total_papers):.1f} per paper avg)")
    print(f"  - Paper ID items: {stats['paper_id_items']} ({(stats['paper_id_items']/total_papers):.1f} per paper)")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python load_data.py <papers_json_path> <table_name> [--region REGION]", file=sys.stderr)
        sys.exit(2)
    main()
