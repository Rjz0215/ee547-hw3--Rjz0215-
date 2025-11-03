#!/usr/bin/env python3
import os, sys, json, argparse, urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
import boto3
from boto3.dynamodb.conditions import Key

AUTHOR_INDEX = "AuthorIndex"
PAPER_INDEX  = "PaperIdIndex"
KEYWORD_INDEX= "KeywordIndex"

def ddb_table(table_name, region):
    session = boto3.Session(region_name=region)
    return session.resource("dynamodb").Table(table_name)

def json_response(handler, code, payload):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)

class Api(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stdout.write("%s - - [%s] %s\n" % (self.client_address[0], self.log_date_time_string(), fmt%args))
        sys.stdout.flush()

    def do_GET(self):
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            qs = urllib.parse.parse_qs(parsed.query)
            region = os.environ.get("AWS_REGION","us-east-1")
            table_name = os.environ.get("ARXIV_TABLE","arxiv-papers")
            table = ddb_table(table_name, region)

            # /papers/recent?category=...&limit=...
            if path == "/papers/recent":
                category = qs.get("category", [None])[0]
                limit = int(qs.get("limit", ["20"])[0])
                if not category:
                    return json_response(self, 400, {"error":"missing category"})
                resp = table.query(
                    KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}'),
                    ScanIndexForward=False,
                    Limit=limit
                )
                items = resp.get("Items", [])
                return json_response(self, 200, {"category": category, "papers": items, "count": len(items)})

            # /papers/author/{author_name}
            if path.startswith("/papers/author/"):
                author_name = urllib.parse.unquote(path.split("/papers/author/")[1])
                resp = table.query(
                    IndexName=AUTHOR_INDEX,
                    KeyConditionExpression=Key('GSI1PK').eq(f'AUTHOR#{author_name}')
                )
                items = resp.get("Items", [])
                return json_response(self, 200, {"author": author_name, "papers": items, "count": len(items)})

            # /papers/{arxiv_id}
            if path.startswith("/papers/") and path.count("/") == 2 and not path.startswith("/papers/author/") and not path.startswith("/papers/keyword/"):
                arxiv_id = urllib.parse.unquote(path.split("/papers/")[1])
                resp = table.query(
                    IndexName=PAPER_INDEX,
                    KeyConditionExpression=Key('GSI2PK').eq(f'PAPER#{arxiv_id}')
                )
                items = resp.get("Items", [])
                if not items:
                    return json_response(self, 404, {"error":"not found"})
                return json_response(self, 200, items[0])

            # /papers/search?category=...&start=YYYY-MM-DD&end=YYYY-MM-DD
            if path == "/papers/search":
                category = qs.get("category", [None])[0]
                start = qs.get("start", [None])[0]
                end = qs.get("end", [None])[0]
                if not category or not start or not end:
                    return json_response(self, 400, {"error":"missing category/start/end"})
                resp = table.query(
                    KeyConditionExpression=Key('PK').eq(f'CATEGORY#{category}') &
                                           Key('SK').between(f'{start}#', f'{end}#zzzzzzz')
                )
                items = resp.get("Items", [])
                return json_response(self, 200, {"category": category, "start": start, "end": end, "papers": items, "count": len(items)})

            # /papers/keyword/{keyword}?limit=...
            if path.startswith("/papers/keyword/"):
                keyword = urllib.parse.unquote(path.split("/papers/keyword/")[1]).lower()
                limit = int(qs.get("limit", ["20"])[0])
                resp = table.query(
                    IndexName=KEYWORD_INDEX,
                    KeyConditionExpression=Key('GSI3PK').eq(f'KEYWORD#{keyword}'),
                    ScanIndexForward=False,
                    Limit=limit
                )
                items = resp.get("Items", [])
                return json_response(self, 200, {"keyword": keyword, "papers": items, "count": len(items)})

            return json_response(self, 404, {"error":"route not found"})
        except Exception as e:
            return json_response(self, 500, {"error": str(e)})

def main():
    ap = argparse.ArgumentParser(description="ArXiv API server (stdlib http.server + DynamoDB)")
    ap.add_argument("port", nargs="?", type=int, default=8080)
    args = ap.parse_args()
    port = args.port
    httpd = HTTPServer(("0.0.0.0", port), Api)
    print(f"Listening on :{port}")
    httpd.serve_forever()

if __name__ == "__main__":
    main()
