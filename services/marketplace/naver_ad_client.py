"""
naver_ad_client.py — 네이버 검색광고 API 클라이언트.

인증: HMAC-SHA256 서명 (API_KEY + SECRET_KEY + CUSTOMER_ID)
Base URL: https://api.searchad.naver.com

네이버 커머스 API(naver_client.py)와 완전히 별개의 인증 체계.
검색광고 API는 OAuth가 아닌 HMAC 서명 기반 인증.
"""
import base64
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timedelta

import requests

logger = logging.getLogger(__name__)

BASE_URL = 'https://api.searchad.naver.com'


class NaverAdClient:
    """네이버 검색광고 API 클라이언트.

    marketplace_api_config와 별도로, 광고 API 전용 설정을 사용한다.
    config keys:
        - ad_customer_id: 검색광고 고객번호
        - ad_api_key: 액세스라이선스
        - ad_secret_key: 비밀키
    """

    def __init__(self, config: dict):
        self.customer_id = str(config.get('ad_customer_id', ''))
        self.api_key = config.get('ad_api_key', '')
        self.secret_key = config.get('ad_secret_key', '')
        self._session = None

    @property
    def is_ready(self) -> bool:
        return bool(self.customer_id and self.api_key and self.secret_key)

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
        return self._session

    # ── 인증 ──

    def _generate_signature(self, timestamp: str, method: str, uri: str) -> str:
        """HMAC-SHA256 서명 생성.

        message = "{timestamp}.{method}.{uri}"
        secret_key를 UTF-8 바이트로 사용하여 HMAC-SHA256 해시 후 base64 인코딩.
        """
        message = f'{timestamp}.{method}.{uri}'
        h = hmac.HMAC(
            self.secret_key.encode('utf-8'),
            message.encode('utf-8'),
            digestmod=hashlib.sha256,
        )
        return base64.b64encode(h.digest()).decode('utf-8')

    def _get_headers(self, method: str, uri: str) -> dict:
        """API 요청 헤더 생성."""
        timestamp = str(int(time.time() * 1000))
        signature = self._generate_signature(timestamp, method, uri)
        return {
            'Content-Type': 'application/json; charset=UTF-8',
            'X-Timestamp': timestamp,
            'X-API-KEY': self.api_key,
            'X-Customer': self.customer_id,
            'X-Signature': signature,
        }

    # ── API 호출 ──

    def _request(self, method: str, uri: str, params: dict = None,
                 json_body: dict = None) -> dict | list | None:
        """공통 API 요청."""
        headers = self._get_headers(method, uri)
        url = f'{BASE_URL}{uri}'
        try:
            resp = self.session.request(
                method, url, headers=headers,
                params=params, json=json_body, timeout=30,
            )
            if resp.status_code == 204:
                return None
            if resp.status_code != 200:
                logger.error(f'[네이버광고] {method} {uri} '
                             f'{resp.status_code}: {resp.text[:300]}')
                return None
            return resp.json()
        except Exception as e:
            logger.error(f'[네이버광고] {method} {uri} 오류: {e}')
            return None

    # ── 캠페인/광고그룹 ──

    def get_campaigns(self) -> list:
        """전체 캠페인 목록."""
        return self._request('GET', '/ncc/campaigns') or []

    def get_adgroups(self, campaign_id: str = None) -> list:
        """광고그룹 목록. campaign_id 지정 시 해당 캠페인만."""
        params = {}
        if campaign_id:
            params['nccCampaignId'] = campaign_id
        return self._request('GET', '/ncc/adgroups', params=params) or []

    # ── 통계 조회 (Stat) ──

    def get_stat(self, ids: list, fields: list = None,
                 date_from: str = None, date_to: str = None) -> list:
        """통계 조회 — 캠페인/광고그룹/키워드 등의 비용·클릭·노출 데이터.

        Args:
            ids: 엔티티 ID 리스트 (캠페인ID, 광고그룹ID 등)
            fields: 요청 필드 (기본: 비용/클릭/노출/전환)
            date_from: 'YYYY-MM-DD' (since, 포함)
            date_to: 'YYYY-MM-DD' (until, 미포함 — 다음날 지정 필요)

        Returns:
            [{id, salesAmt, clkCnt, impCnt, ccnt, convAmt, ...}, ...]
        """
        if not fields:
            fields = [
                'impCnt', 'clkCnt', 'salesAmt', 'cpc', 'ctr',
                'ccnt', 'crto', 'convAmt', 'ror', 'cpConv',
            ]
        if not date_from:
            date_from = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        if not date_to:
            date_to = datetime.now().strftime('%Y-%m-%d')

        params = {
            'ids': ','.join(ids),
            'fields': json.dumps(fields) if isinstance(fields, list) else fields,
            'timeRange': json.dumps({
                'since': date_from, 'until': date_to,
            }),
        }
        result = self._request('GET', '/stats', params=params)
        if isinstance(result, dict):
            return result.get('data', [])
        return result or []

    # ── StatReport (대용량 보고서) ──

    def create_stat_report(self, report_type: str = 'AD',
                           date_from: str = None, date_to: str = None) -> dict | None:
        """대용량 보고서 생성 요청.

        Args:
            report_type: 'AD', 'AD_DETAIL', 'KEYWORD', 'CAMPAIGN_BUDGET' 등
            date_from: 'YYYY-MM-DD'
            date_to: 'YYYY-MM-DD'

        Returns:
            {reportJobId, status, ...} or None
        """
        if not date_from:
            date_from = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        if not date_to:
            date_to = datetime.now().strftime('%Y-%m-%d')

        body = {
            'reportTp': report_type,
            'statDt': date_from.replace('-', ''),
            'endDt': date_to.replace('-', ''),
        }
        return self._request('POST', '/stat-reports', json_body=body)

    def get_stat_reports(self) -> list:
        """보고서 목록 조회."""
        return self._request('GET', '/stat-reports') or []

    def get_stat_report(self, report_job_id: str) -> dict | None:
        """보고서 상태/URL 조회."""
        return self._request('GET', f'/stat-reports/{report_job_id}')

    def download_stat_report(self, download_url: str) -> str | None:
        """보고서 다운로드 (TSV 텍스트 반환)."""
        try:
            resp = self.session.get(download_url, timeout=60)
            if resp.status_code == 200:
                return resp.text
            logger.error(f'[네이버광고] 보고서 다운로드 실패: {resp.status_code}')
            return None
        except Exception as e:
            logger.error(f'[네이버광고] 보고서 다운로드 오류: {e}')
            return None

    # ── 일별 광고비 집계 ──

    def fetch_daily_ad_cost(self, date_from: str, date_to: str) -> list:
        """캠페인별 일별 광고비 조회 → 정산용 집계 데이터 반환.

        /stats API는 기간 합산만 제공하므로 하루씩 호출하여 일별 데이터를 수집.
        [{date, campaign_id, campaign_name, cost, clicks, impressions, conversions}, ...]
        형태로 반환.
        """
        campaigns = self.get_campaigns()
        if not campaigns:
            logger.warning('[네이버광고] 캠페인 없음')
            return []

        campaign_ids = [c['nccCampaignId'] for c in campaigns]
        campaign_names = {c['nccCampaignId']: c.get('name', '') for c in campaigns}

        # 하루씩 호출 (since=당일, until=다음날)
        results = []
        dt = datetime.strptime(date_from, '%Y-%m-%d')
        dt_end = datetime.strptime(date_to, '%Y-%m-%d')

        while dt < dt_end:
            d_str = dt.strftime('%Y-%m-%d')
            d_next = (dt + timedelta(days=1)).strftime('%Y-%m-%d')

            stats = self.get_stat(
                ids=campaign_ids,
                fields=['impCnt', 'clkCnt', 'salesAmt', 'ccnt', 'convAmt'],
                date_from=d_str,
                date_to=d_next,
            )
            for item in stats:
                cid = item.get('id', '')
                cost = int(item.get('salesAmt', 0))
                clicks = int(item.get('clkCnt', 0))
                if cost > 0 or clicks > 0:
                    results.append({
                        'date': d_str,
                        'campaign_id': cid,
                        'campaign_name': campaign_names.get(cid, ''),
                        'cost': cost,
                        'clicks': clicks,
                        'impressions': int(item.get('impCnt', 0)),
                        'conversions': int(item.get('ccnt', 0)),
                        'conversion_amount': int(item.get('convAmt', 0)),
                    })
            dt += timedelta(days=1)

        logger.info(f'[네이버광고] 일별 광고비 {len(results)}건 조회 '
                     f'({date_from}~{date_to})')
        return results
