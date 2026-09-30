import os
import xml.etree.ElementTree as ET
from datetime import datetime

import boto3
import requests

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup


LAWD_CODES = [
    "11680",
    "11650",
    "11710",
    "11440",
    "11170",
    "11200",
]

API_URL = (
    "https://apis.data.go.kr/1613000/"
    "RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
)

S3_BUCKET = os.getenv("S3_BUCKET", "realestate-parkdaeun")

def collect_realestate(lawd_cd, **context):
    print(f"collector=박다은, time={datetime.now()}, lawd={lawd_cd}")

    service_key = os.getenv("DATA_GO_KR_KEY")

    if not service_key:
        raise ValueError("DATA_GO_KR_KEY 환경변수가 없습니다.")

    yyyymm = context["logical_date"].strftime("%Y%m")

    params = {
        "serviceKey": service_key,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": yyyymm,
        "numOfRows": 9999,
        "pageNo": 1,
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=60,
    )
    response.raise_for_status()

    raw_xml = response.text

    # 뒤의 Branch/S3 저장에서 사용
    context["ti"].xcom_push(
        key=f"raw_xml_{lawd_cd}",
        value=raw_xml,
    )

    context["ti"].xcom_push(
        key=f"yyyymm_{lawd_cd}",
        value=yyyymm,
    )

    print(
        f"API collection completed: "
        f"LAWD_CD={lawd_cd}, DEAL_YMD={yyyymm}"
    )


def choose_branch(**context):
    ti = context["ti"]

    for lawd_cd in LAWD_CODES:
        raw_xml = ti.xcom_pull(
            task_ids=f"collect_group.collect_{lawd_cd}",
            key=f"raw_xml_{lawd_cd}",
        )

        # 응답이 없는 경우
        if not raw_xml:
            print(f"EMPTY RESPONSE: {lawd_cd}")
            return "skip_upload"

        # XML 파싱 실패
        try:
            root = ET.fromstring(raw_xml)
        except ET.ParseError:
            print(f"XML PARSE ERROR: {lawd_cd}")
            return "skip_upload"

        # API 오류 응답
        result_code = root.findtext(".//resultCode")

        if result_code and result_code not in ("00", "000"):
            print(
                f"API ERROR: "
                f"LAWD_CD={lawd_cd}, resultCode={result_code}"
            )
            return "skip_upload"

        # 데이터 0건
        items = root.findall(".//item")

        if len(items) == 0:
            print(f"ZERO ITEMS: {lawd_cd}")
            return "skip_upload"

    # 6개 지역 모두 정상
    return "upload_to_s3"


def save_all_to_s3(**context):
    ti = context["ti"]
    s3 = boto3.client("s3")

    for lawd_cd in LAWD_CODES:
        raw_xml = ti.xcom_pull(
            task_ids=f"collect_group.collect_{lawd_cd}",
            key=f"raw_xml_{lawd_cd}",
        )

        yyyymm = ti.xcom_pull(
            task_ids=f"collect_group.collect_{lawd_cd}",
            key=f"yyyymm_{lawd_cd}",
        )

        s3_key = f"bronze/{yyyymm}/{lawd_cd}.xml"

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=raw_xml.encode("utf-8"),
            ContentType="application/xml",
        )

        print(f"saved: s3://{S3_BUCKET}/{s3_key}")


with DAG(
    dag_id="bronze_realestate_collect",
    schedule="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=True,
    tags=["realestate", "bronze"],
) as dag:

    # 6개 시군구 병렬 수집
    with TaskGroup(group_id="collect_group") as collect_group:

        collect_tasks = []

        for lawd_cd in LAWD_CODES:
            collect_task = PythonOperator(
                task_id=f"collect_{lawd_cd}",
                python_callable=collect_realestate,
                op_kwargs={"lawd_cd": lawd_cd},
            )

            collect_tasks.append(collect_task)

    # 수집 결과에 따라 분기
    branch_after_collect = BranchPythonOperator(
        task_id="branch_after_collect",
        python_callable=choose_branch,
    )

    # 정상일 경우 S3 저장
    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=save_all_to_s3,
    )

    # 실패/0건일 경우
    skip_upload = EmptyOperator(
        task_id="skip_upload",
    )

    # 최종 연결
    collect_group >> branch_after_collect
    branch_after_collect >> [upload_to_s3, skip_upload]