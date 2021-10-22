from typing import Any, Dict, Optional

import pandas as pd
from IPython.core import magic
from IPython.core.getipython import get_ipython
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from pyspark.sql import DataFrame as SparkDataFrame

from ..spark.aggsum import AggSum, AggSumResult
from ..spark.dp import Dp, DpResult
from ..spark.dq import Dq, DqResult
from ..utils.spark_session import get_or_create_spark_session


@magic.magics_class
class IpythonMagics(magic.Magics):
    _table_name_help = """
        Table name is used to register the result as Spark (temporary) view and Pyhton variable.
        BEWARE: It will overwrite any existing view / variable of the same name.
    """.strip()

    @magic.cell_magic
    @magic.needs_local_scope
    @magic_arguments()
    @argument("-dl", "--display_limit", type=int, nargs="?", default=1_000, help="Maximum number of rows to display.")
    @argument("table_name", type=str, nargs="?", help=_table_name_help)
    def ssql(self, line: str, cell: str, local_ns: Dict[str, Any]) -> pd.DataFrame:
        """Execute Spark SQL command and display the result."""
        args = parse_argstring(self.ssql, line)
        sdf = self._execute_sql(cell, local_ns, args.table_name)
        sdf._repr_html_ = lambda: self._safe_limit(sdf, args.display_limit).toPandas()._repr_html_()
        return sdf

    @magic.cell_magic
    @magic.needs_local_scope
    def ssql_dq(self, line: str, cell: str, local_ns: Dict[str, Any]) -> DqResult:
        """Compute DQ (quality) from SQL and display the result."""
        sql = cell.format(**local_ns)
        return Dq.fromDqSql(get_or_create_spark_session(), sql).dqResult()

    @magic.cell_magic
    @magic.needs_local_scope
    def ssql_dp(self, line: str, cell: str, local_ns: Dict[str, Any]) -> DpResult:
        """Compute DP (profile) from SQL and display the result."""
        sdf = self._execute_sql(cell, local_ns, table_name=None)
        return Dp.fromDataFrame(sdf).dpResult()

    @magic.cell_magic
    @magic.needs_local_scope
    def ssql_aggsum(self, line: str, cell: str, local_ns: Dict[str, Any]) -> AggSumResult:
        """Compute AggSum (aggregate summary) from SQL and display the result."""
        sdf = self._execute_sql(cell, local_ns, table_name=None)
        return AggSum.fromDataFrame(sdf).aggSumResult()

    def _execute_sql(self, sql: str, local_ns: Dict[str, Any], table_name: Optional[str]) -> SparkDataFrame:
        sql = sql.format(**local_ns)
        sdf = get_or_create_spark_session().sql(sql)
        if table_name is not None:
            sdf = sdf.alias(table_name)
            sdf.createOrReplaceTempView(table_name)
            # globals()[args.table_name] = sdf
            get_ipython().push({table_name: sdf})
        return sdf

    def _safe_limit(self, sdf: SparkDataFrame, limit: int) -> SparkDataFrame:
        return sdf if limit is None or limit <= 0 else sdf.limit(limit)


def init() -> None:
    get_ipython().register_magics(IpythonMagics)
