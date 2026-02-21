"""
db_base.py — DB 인터페이스 정의 (Abstract Base)
"""
from abc import ABC, abstractmethod


class DBBase(ABC):
    """DB 접근 인터페이스"""

    @abstractmethod
    def connect(self):
        """DB 연결. 성공 시 True, 실패 시 False."""
        ...

    @abstractmethod
    def get_db_columns(self):
        """stock_ledger 테이블의 컬럼 set 반환 (None=알 수 없음)."""
        ...

    # --- stock_ledger CRUD ---
    @abstractmethod
    def insert_stock_ledger(self, payload_list):
        ...

    @abstractmethod
    def delete_stock_ledger_all(self):
        """전체 삭제. 삭제 건수 반환."""
        ...

    @abstractmethod
    def delete_stock_ledger_by(self, date_str, record_type, location=None):
        """조건부 삭제. 삭제 건수 반환."""
        ...

    @abstractmethod
    def query_stock_ledger(self, date_to, date_from=None, location=None,
                            category=None, type_list=None, order_desc=False):
        """stock_ledger 조회. list of dict 반환."""
        ...

    @abstractmethod
    def query_stock_by_location(self, location, select_fields=None):
        """특정 창고 재고 조회. list of dict 반환."""
        ...

    @abstractmethod
    def query_filter_options(self):
        """고유 창고/종류 목록. (locations, categories) 반환."""
        ...

    @abstractmethod
    def query_unit_for_product(self, product_name):
        """품목의 DB 단위 반환. 없으면 None."""
        ...

    @abstractmethod
    def update_stock_ledger(self, row_id, update_data):
        ...

    @abstractmethod
    def delete_stock_ledger_by_id(self, row_id):
        ...

    # --- daily_revenue CRUD ---
    @abstractmethod
    def upsert_revenue(self, payload_list):
        ...

    @abstractmethod
    def query_revenue(self, date_from=None, date_to=None, category=None):
        """매출 조회. list of dict 반환."""
        ...

    @abstractmethod
    def delete_revenue_all(self):
        ...

    @abstractmethod
    def delete_revenue_by_date(self, date_from=None, date_to=None):
        ...

    # --- master tables ---
    @abstractmethod
    def sync_master_table(self, table_name, payload_list, batch_size=500):
        """전체 교체(삭제→삽입)."""
        ...

    @abstractmethod
    def query_master_table(self, table_name):
        ...

    @abstractmethod
    def count_master_table(self, table_name):
        ...
