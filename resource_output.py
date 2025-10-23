#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AWS 인프라 구성 집계 스크립트 (Python 3.13)
- 계정명 / 지역 / 서비스 / 타입 / 총 갯수 / 동작 상태별 갯수
- 리전 순회 자원 + 글로벌 자원 집계
- 콘솔 요약 출력 + Excel 저장

포함 서비스:
EC2, EBS, S3, RDS, ElastiCache(Redis), DynamoDB, Route53, ELB(ALB/NLB/Classic),
CloudFront, Lambda, EKS, ECS, ECR, API Gateway(v1/v2), SQS, SNS,
OpenSearch(Service/Serverless)

pip install boto3 botocore openpyxl
"""

import sys
from collections import defaultdict, Counter
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
from botocore.exceptions import ClientError # 권한 없는 서비스 건너 뛰고 수행용

# ============================================================
# 0) 자격증명 하드코딩
# ============================================================
ACCESS_KEY = "엑세스키"
SECRET_KEY = "시크릿키"
SESSION_TOKEN = None  # 필요 없으면 None
ACCOUNT_ALIAS_OR_NAME = "AWS Account명(숫자)"
# ============================================================
# 1) 공통 설정
# ============================================================
BOTO_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    connect_timeout=10,
    read_timeout=60,
    user_agent_extra="infra-inventory/1.2-region-col"
)

def make_session():
    return boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN
    )

def list_all_regions(session):
    """상용 리전 리스트"""
    ec2 = session.client("ec2", region_name="us-east-1", config=BOTO_CONFIG)
    resp = ec2.describe_regions(AllRegions=False)
    return sorted([r["RegionName"] for r in resp["Regions"]])

# ============================================================
# 2) 헬퍼
# ============================================================
def safe_get(client_call, **kwargs):
    try:
        return client_call(**kwargs)
    except ClientError as e:
        print(f"[WARN] API error: {e}", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"[WARN] Error: {e}", file=sys.stderr)
        return {}

def dict_counter_add(counter: Counter, key: str, inc: int = 1):
    counter[key] += inc

def summarize_counter(counter: Counter) -> tuple[int, str]:
    total = sum(counter.values())
    parts = [f"{k}:{v}" for k, v in sorted(counter.items())]
    return total, " | ".join(parts) if parts else ""

#아래 4개 함수는 권한이 없어도 스크립트가 중간에 죽지않고 가능한 항목만 집계하도록 방어로직
def _is_access_denied(e: ClientError) -> bool:
    try:
        code = e.response.get("Error", {}).get("Code", "")
    except Exception:
        code = ""
    return code in (
        "UnauthorizedOperation",
        "AccessDenied",
        "AccessDeniedException",
        "UnrecognizedClientException",
        "ExpiredTokenException"
    )

def _is_access_denied_exc(e: Exception) -> bool:
    if not isinstance(e, ClientError):
        return False
    code = e.response.get("Error", {}).get("Code", "")
    return code in {
        "UnauthorizedOperation",
        "AccessDenied",
        "AccessDeniedException",
        "UnrecognizedClientException",
        "ExpiredTokenException",
        "AccessDeniedForDependencyException",
    }

def safe_paginate(paginator, *, on_skip:str="", **kwargs):
    """
    안전한 페이지네이터.
    - 권한/토큰 오류: 경고만 출력하고 '빈 반복'을 반환하여 상위 로직이 계속 진행되도록.
    - 다른 오류: 그대로 raise (디버깅을 위해)
    """
    try:
        for page in paginator.paginate(**kwargs):
            yield page
    except Exception as e:
        if _is_access_denied_exc(e):
            msg = e.response.get("Error", {}).get("Message", str(e))
            print(f"[SKIP] {on_skip or 'paginate'}: AccessDenied → {msg}")
            return
        raise

def safe_call(fn, *, on_skip:str="", **kwargs):
    """
    단건 API 안전 호출.
    - 권한/토큰 오류: 경고 출력 후 빈 dict 반환
    - 다른 오류: 그대로 raise
    """
    try:
        return fn(**kwargs)
    except Exception as e:
        if _is_access_denied_exc(e):
            msg = getattr(e, "response", {}).get("Error", {}).get("Message", str(e))
            print(f"[SKIP] {on_skip or getattr(fn, '__name__', 'call')}: AccessDenied → {msg}")
            return {}
        raise

# for page in paginator.paginate(): -> for page in safe_paginate(paginator): 대체

# ============================================================
# 3) 수집기 (리전형 / 글로벌형)
#   각 함수는 (계정명, 지역, 서비스, 타입, 총, 상세) 튜플 리스트를 반환
# ============================================================

def collect_ec2(session, regions):
    rows = []
    for region in regions:
        ec2 = session.client("ec2", region_name=region, config=BOTO_CONFIG)
        counter = Counter()
        paginator = ec2.get_paginator("describe_instances")
        for page in safe_paginate(paginator):
            for r in page.get("Reservations", []):
                for i in r.get("Instances", []):
                    state = i.get("State", {}).get("Name", "unknown")
                    dict_counter_add(counter, state)
        total, details = summarize_counter(counter)
        if total:
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "EC2", "Instances", total, details))
            print(f"[EC2][{region}] total={total}, {details}")
    return rows

def collect_ebs(session, regions):
    rows = []
    for region in regions:
        ec2 = session.client("ec2", region_name=region, config=BOTO_CONFIG)
        counter = Counter()
        paginator = ec2.get_paginator("describe_volumes")
        for page in safe_paginate(paginator):
            for v in page.get("Volumes", []):
                state = v.get("State", "unknown")  # in-use / available
                dict_counter_add(counter, state)
        total, details = summarize_counter(counter)
        if total:
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "EBS", "Volumes", total, details))
            print(f"[EBS][{region}] total={total}, {details}")
    return rows

def collect_s3(session):
    region = "global"
    s3 = session.client("s3", config=BOTO_CONFIG)
    counter = Counter()
    resp = safe_get(s3.list_buckets)
    for b in resp.get("Buckets", []):
        name = b["Name"]
        ver = safe_get(s3.get_bucket_versioning, Bucket=name)
        status = ver.get("Status", "Unversioned")  # Enabled / Suspended / Unversioned
        dict_counter_add(counter, status)
    total, details = summarize_counter(counter)
    rows = []
    if total:
        rows.append((ACCOUNT_ALIAS_OR_NAME, region, "S3", "Buckets", total, details))
        print(f"[S3][{region}] total={total}, {details}")
    return rows

def collect_rds(session, regions):
    rows = []
    for region in regions:
        rds = session.client("rds", region_name=region, config=BOTO_CONFIG)
        engine_state = defaultdict(Counter)
        paginator = rds.get_paginator("describe_db_instances")
        for page in safe_paginate(paginator):
            for db in page.get("DBInstances", []):
                engine = db.get("Engine", "unknown")
                status = db.get("DBInstanceStatus", "unknown")
                engine_state[engine][status] += 1
        for eng, cnt in engine_state.items():
            total, details = summarize_counter(cnt)
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "RDS", eng, total, details))
            print(f"[RDS][{region}][{eng}] total={total}, {details}")
    return rows

def collect_elasticache_redis(session, regions):
    rows = []
    for region in regions:
        ec = session.client("elasticache", region_name=region, config=BOTO_CONFIG)
        counter = Counter()
        paginator = ec.get_paginator("describe_replication_groups")
        for page in safe_paginate(paginator):
            for rg in page.get("ReplicationGroups", []):
                status = rg.get("Status", "unknown")
                dict_counter_add(counter, status)
        total, details = summarize_counter(counter)
        if total:
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "ElastiCache", "Redis", total, details))
            print(f"[ElastiCache-Redis][{region}] total={total}, {details}")
    return rows

def collect_dynamodb(session, regions):
    rows = []
    for region in regions:
        ddb = session.client("dynamodb", region_name=region, config=BOTO_CONFIG)
        state_counter = Counter()
        billing_counter = Counter()
        paginator = ddb.get_paginator("list_tables")
        for page in safe_paginate(paginator):
            for name in page.get("TableNames", []):
                desc = safe_get(ddb.describe_table, TableName=name)
                td = desc.get("Table", {})
                state = td.get("TableStatus", "unknown")
                dict_counter_add(state_counter, state)
                billing = td.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED")
                dict_counter_add(billing_counter, billing)
        total_s, details_s = summarize_counter(state_counter)
        total_b, details_b = summarize_counter(billing_counter)
        if total_s:
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "DynamoDB", "Tables", total_s, details_s))
            print(f"[DynamoDB][{region}] tables={total_s}, states: {details_s}")
        if total_b:
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "DynamoDB", "BillingMode", total_b, details_b))
            print(f"[DynamoDB][{region}] billing: {details_b}")
    return rows

def collect_route53(session):
    region = "global"
    r53 = session.client("route53", config=BOTO_CONFIG)
    counter = Counter()
    paginator = r53.get_paginator("list_hosted_zones")
    for page in safe_paginate(paginator):
        for hz in page.get("HostedZones", []):
            private = hz.get("Config", {}).get("PrivateZone", False)
            dict_counter_add(counter, "Private" if private else "Public")
    total, details = summarize_counter(counter)
    rows = []
    if total:
        rows.append((ACCOUNT_ALIAS_OR_NAME, region, "Route53", "HostedZones", total, details))
        print(f"[Route53][{region}] zones={total}, {details}")
    return rows

def collect_elb(session, regions):
    rows = []
    for region in regions:
        # ---- ELBv2 (ALB/NLB/GWLB) ----
        elbv2 = session.client("elbv2", region_name=region, config=BOTO_CONFIG)
        v2_counter = defaultdict(Counter)

        try:
            paginator = elbv2.get_paginator("describe_load_balancers")
            for page in safe_paginate(paginator, on_skip=f"ELBv2[{region}].describe_load_balancers"):
                for lb in page.get("LoadBalancers", []):
                    lb_type = lb.get("Type", "unknown")   # application / network / gateway
                    state = lb.get("State", {}).get("Code", "unknown")
                    v2_counter[lb_type][state] += 1
        except Exception as e:
            # 비권한 오류 외의 예외는 한 리전만 스킵하고 다음 리전 계속
            print(f"[WARN] ELBv2[{region}] unexpected error: {e}")

        for t, c in v2_counter.items():
            total = sum(c.values())
            if not total:
                continue
            type_label = "ALB" if t == "application" else ("NLB" if t == "network" else ("GWLB" if t == "gateway" else t))
            details = " | ".join(f"{k}:{v}" for k, v in sorted(c.items()))
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "ELB", type_label, total, details))
            print(f"[ELBv2][{region}][{type_label}] total={total}, {details}")

        # ---- Classic ELB ----
        elb = session.client("elb", region_name=region, config=BOTO_CONFIG)
        c_counter = Counter()
        try:
            paginator2 = elb.get_paginator("describe_load_balancers")
            for page in safe_paginate(paginator2, on_skip=f"ELB-Classic[{region}].describe_load_balancers"):
                # Classic은 상태코드가 별도 노출되지 않으므로 presence만 집계
                for _ in page.get("LoadBalancerDescriptions", []):
                    c_counter["present"] += 1
        except Exception as e:
            print(f"[WARN] ELB-Classic[{region}] unexpected error: {e}")

        if sum(c_counter.values()) > 0:
            total = sum(c_counter.values())
            details = " | ".join(f"{k}:{v}" for k, v in sorted(c_counter.items()))
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "ELB", "Classic", total, details))
            print(f"[ELB-Classic][{region}] total={total}, {details}")

    return rows


def collect_cloudfront(session):
    region = "global"
    cf = session.client("cloudfront", config=BOTO_CONFIG)
    counter = Counter()
    paginator = cf.get_paginator("list_distributions")
    for page in safe_paginate(paginator):
        dist_list = page.get("DistributionList", {})
        for item in dist_list.get("Items", []) or []:
            enabled = item.get("Enabled", False)
            dict_counter_add(counter, "Enabled" if enabled else "Disabled")
    total, details = summarize_counter(counter)
    rows = []
    if total:
        rows.append((ACCOUNT_ALIAS_OR_NAME, region, "CloudFront", "Distributions", total, details))
        print(f"[CloudFront][{region}] total={total}, {details}")
    return rows

def collect_lambda(session, regions):
    rows = []
    for region in regions:
        lm = session.client("lambda", region_name=region, config=BOTO_CONFIG)
        runtime_counter = Counter()
        state_counter = Counter()
        paginator = lm.get_paginator("list_functions")
        for page in safe_paginate(paginator):
            for f in page.get("Functions", []):
                runtime = f.get("Runtime", "unknown")
                dict_counter_add(runtime_counter, runtime)
                state = f.get("State", "Unknown")
                dict_counter_add(state_counter, state)
        total_r, details_r = summarize_counter(runtime_counter)
        total_s, details_s = summarize_counter(state_counter)
        if total_r:
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "Lambda", "Runtime", total_r, details_r))
            print(f"[Lambda][{region}] runtimes={total_r}, {details_r}")
        if total_s:
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "Lambda", "State", total_s, details_s))
            print(f"[Lambda][{region}] states={details_s}")
    return rows

def collect_eks(session, regions):
    rows = []
    for region in regions:
        eks = session.client("eks", region_name=region, config=BOTO_CONFIG)
        state_counter = Counter()
        paginator = eks.get_paginator("list_clusters")
        for page in safe_paginate(paginator):
            for name in page.get("clusters", []):
                desc = safe_get(eks.describe_cluster, name=name)
                status = (desc.get("cluster") or {}).get("status", "unknown")
                dict_counter_add(state_counter, status)
        if sum(state_counter.values()):
            total, details = summarize_counter(state_counter)
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "EKS", "Clusters", total, details))
            print(f"[EKS][{region}] clusters={total}, {details}")
    return rows

def collect_ecs(session, regions):
    rows = []
    for region in regions:
        ecs = session.client("ecs", region_name=region, config=BOTO_CONFIG)
        # 클러스터
        cluster_state = Counter()
        clusters = []
        paginator = ecs.get_paginator("list_clusters")
        for page in safe_paginate(paginator):
            clusters += page.get("clusterArns", [])
        if clusters:
            for i in range(0, len(clusters), 100):
                part = clusters[i:i+100]
                resp = safe_get(ecs.describe_clusters, clusters=part, include=['STATISTICS'])
                for c in resp.get("clusters", []):
                    status = c.get("status", "UNKNOWN")  # ACTIVE/INACTIVE
                    dict_counter_add(cluster_state, status)
        if sum(cluster_state.values()):
            total, details = summarize_counter(cluster_state)
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "ECS", "Clusters", total, details))
            print(f"[ECS][{region}] clusters={total}, {details}")

        # 서비스
        service_state = Counter()
        for arn in clusters:
            svc_arns = []
            paginator = ecs.get_paginator("list_services")
            for page in paginator.paginate(cluster=arn):
                svc_arns += page.get("serviceArns", [])
            for i in range(0, len(svc_arns), 10):
                part = svc_arns[i:i+10]
                resp = safe_get(ecs.describe_services, cluster=arn, services=part)
                for s in resp.get("services", []):
                    status = s.get("status", "UNKNOWN")
                    dict_counter_add(service_state, status)
        if sum(service_state.values()):
            total, details = summarize_counter(service_state)
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "ECS", "Services", total, details))
            print(f"[ECS][{region}] services={total}, {details}")
    return rows

def collect_ecr(session, regions):
    rows = []
    for region in regions:
        ecr = session.client("ecr", region_name=region, config=BOTO_CONFIG)
        # 리포지토리
        repos = []
        paginator = ecr.get_paginator("describe_repositories")
        for page in safe_paginate(paginator):
            repos += page.get("repositories", [])
        repo_total = len(repos)
        if repo_total:
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "ECR", "Repositories", repo_total, ""))
            print(f"[ECR][{region}] repositories={repo_total}")

        # 이미지 (TAGGED / UNTAGGED)
        img_counter = Counter()
        for r in repos:
            name = r.get("repositoryName")
            # TAGGED
            paginator_t = ecr.get_paginator("list_images")
            for page in paginator_t.paginate(repositoryName=name, filter={"tagStatus": "TAGGED"}):
                img_counter["TAGGED"] += len(page.get("imageIds", []))
            # UNTAGGED
            paginator_u = ecr.get_paginator("list_images")
            for page in paginator_u.paginate(repositoryName=name, filter={"tagStatus": "UNTAGGED"}):
                img_counter["UNTAGGED"] += len(page.get("imageIds", []))
        if sum(img_counter.values()):
            total, details = summarize_counter(img_counter)
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "ECR", "Images", total, details))
            print(f"[ECR][{region}] images={total}, {details}")
    return rows

def collect_apigw(session, regions):
    rows = []
    for region in regions:
        # v1 (REST)
        apigw = session.client("apigateway", region_name=region, config=BOTO_CONFIG)
        rest_total = 0
        paginator = apigw.get_paginator("get_rest_apis")
        for page in safe_paginate(paginator):
            rest_total += len(page.get("items", []))
        if rest_total:
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "API Gateway", "REST", rest_total, "REST:{}".format(rest_total)))
            print(f"[APIGWv1][{region}] REST total={rest_total}")

        # v2 (HTTP/WEBSOCKET)
        apigw2 = session.client("apigatewayv2", region_name=region, config=BOTO_CONFIG)
        proto_counter = Counter()
        paginator2 = apigw2.get_paginator("list_apis")
        for page in paginator2.paginate():
            for api in page.get("Items", []):
                proto = api.get("ProtocolType", "UNKNOWN")  # HTTP/WEBSOCKET
                dict_counter_add(proto_counter, proto)
        if sum(proto_counter.values()):
            total, details = summarize_counter(proto_counter)
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "API Gateway", "v2", total, details))
            print(f"[APIGWv2][{region}] {details}")
    return rows

def collect_sqs(session, regions):
    rows = []
    for region in regions:
        sqs = session.client("sqs", region_name=region, config=BOTO_CONFIG)
        urls = []
        resp = safe_get(sqs.list_queues)
        if resp and resp.get("QueueUrls"):
            urls = resp["QueueUrls"]
        counter = Counter()
        for u in urls:
            if u.endswith(".fifo"):
                counter["FIFO"] += 1
            else:
                counter["Standard"] += 1
        if sum(counter.values()):
            total, details = summarize_counter(counter)
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "SQS", "Queues", total, details))
            print(f"[SQS][{region}] total={total}, {details}")
    return rows

def collect_sns(session, regions):
    rows = []
    for region in regions:
        sns = session.client("sns", region_name=region, config=BOTO_CONFIG)
        total = 0
        paginator = sns.get_paginator("list_topics")
        for page in safe_paginate(paginator):
            total += len(page.get("Topics", []))
        if total:
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "SNS", "Topics", total, f"Topics:{total}"))
            print(f"[SNS][{region}] topics={total}")
    return rows

def collect_opensearch(session, regions):
    rows = []
    for region in regions:
        # OpenSearch Service (도메인)
        oss = session.client("opensearch", region_name=region, config=BOTO_CONFIG)
        names_resp = safe_get(oss.list_domain_names)
        domain_names = [d.get("DomainName") for d in names_resp.get("DomainNames", [])]
        state_counter = Counter()
        version_counter = Counter()
        if domain_names:
            for name in domain_names:
                d = safe_get(oss.describe_domain, DomainName=name)
                st = (d.get("DomainStatus") or {})
                processing = st.get("Processing", False)
                endpoint = st.get("Endpoint")
                state = "Processing" if processing else ("Active" if endpoint else "Created")
                dict_counter_add(state_counter, state)
                eng = st.get("EngineVersion", "unknown")
                dict_counter_add(version_counter, eng)
        if sum(state_counter.values()):
            total, details = summarize_counter(state_counter)
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "OpenSearch", "Domains", total, details))
            print(f"[OpenSearch@Service][{region}] domains={total}, {details}")
        if sum(version_counter.values()):
            total, details = summarize_counter(version_counter)
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "OpenSearch", "EngineVersion", total, details))
            print(f"[OpenSearch@Service][{region}] versions={details}")

        # OpenSearch Serverless (컬렉션)
        ossv = session.client("opensearchserverless", region_name=region, config=BOTO_CONFIG)
        coll_counter = Counter()
        paginator = ossv.get_paginator("list_collections")
        for page in safe_paginate(paginator):
            for c in page.get("collectionSummaries", []):
                status = c.get("status", "UNKNOWN")
                dict_counter_add(coll_counter, status)
        if sum(coll_counter.values()):
            total, details = summarize_counter(coll_counter)
            rows.append((ACCOUNT_ALIAS_OR_NAME, region, "OpenSearch-Serverless", "Collections", total, details))
            print(f"[OpenSearch@Serverless][{region}] collections={total}, {details}")
    return rows

# ============================================================
# 4) 엑셀 저장
# ============================================================
def save_to_excel(rows, outfile):
    wb = Workbook()
    ws = wb.active
    ws.title = "Inventory"

    headers = ["계정명", "지역", "서비스명", "타입", "총 갯수", "동작 상태별 갯수", "생성시각(UTC)"]
    ws.append(headers)

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for acc, region, svc, typ, total, details in rows:
        ws.append([acc, region, svc, typ, total, details, ts])

    for col_idx, width in enumerate([18, 16, 22, 24, 10, 90, 20], start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = width
    for r in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=7):
        for cell in r:
            cell.alignment = Alignment(vertical="top", wrap_text=True)

    wb.save(outfile)
    print(f"[OK] Excel 저장 완료 : {outfile}")

def _run_collect(collector, session, regions_or_none, sink_rows, label):
    try:
        if regions_or_none is None:
            sink_rows += collector(session)  # 글로벌형
        else:
            sink_rows += collector(session, regions_or_none)  # 리전형
    except Exception as e:
        print(f"[WARN] collector {label} failed (continue): {e}")

# ============================================================
# 5) 메인
# ============================================================
def main():
    print("[INFO] 세션 생성…")
    session = make_session()

    print("[INFO] 리전 조회…")
    regions = list_all_regions(session)

    all_rows = []

    # 리전형
    _run_collect(collect_ec2, session, regions, all_rows, "EC2")
    _run_collect(collect_ebs, session, regions, all_rows, "EBS")
    _run_collect(collect_rds, session, regions, all_rows, "RDS")
    _run_collect(collect_elasticache_redis, session, regions, all_rows, "ElastiCache-Redis")
    _run_collect(collect_dynamodb, session, regions, all_rows, "DynamoDB")
    _run_collect(collect_elb, session, regions, all_rows, "ELB")
    _run_collect(collect_lambda, session, regions, all_rows, "Lambda")
    _run_collect(collect_eks, session, regions, all_rows, "EKS")
    _run_collect(collect_ecs, session, regions, all_rows, "ECS")
    _run_collect(collect_ecr, session, regions, all_rows, "ECR")
    _run_collect(collect_apigw, session, regions, all_rows, "APIGW")
    _run_collect(collect_sqs, session, regions, all_rows, "SQS")
    _run_collect(collect_sns, session, regions, all_rows, "SNS")
    _run_collect(collect_opensearch, session, regions, all_rows, "OpenSearch")

    # 글로벌형
    _run_collect(collect_s3, session, None, all_rows, "S3")
    _run_collect(collect_route53, session, None, all_rows, "Route53")
    _run_collect(collect_cloudfront, session, None, all_rows, "CloudFront")

    if not all_rows:
        print("[WARN] 수집된 리소스가 없습니다. (권한/리전/SCP 확인)")
        return

    out_name = f"aws_inventory_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    save_to_excel(all_rows, out_name)

if __name__ == "__main__":
    main()
