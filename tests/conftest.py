import os

import pytest
from pyspark.sql import SparkSession
from pytest import TempPathFactory


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory: TempPathFactory) -> SparkSession:
    import stoys

    stoys.init()

    builder = SparkSession.builder
    builder.master("local[1]")
    builder.appName(os.path.basename(__file__))

    # https://spark.apache.org/docs/latest/configuration.html
    builder.config("spark.master.rest.enabled", "false")
    # builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    # builder.config('spark.sql.codegen.wholeStage', 'false')
    builder.config("spark.sql.shuffle.partitions", "1")
    builder.config("spark.sql.warehouse.dir", "target/spark-warehouse")
    builder.config("spark.ui.enabled", "false")

    # event_log_dir = tmp_path_factory.getbasetemp().parent.joinpath(f'{tmp_path_factory.getbasetemp().name}.event-log')
    # event_log_dir = tmp_path_factory.getbasetemp().joinpath('.event-log')
    # event_log_dir.mkdir(parents=True, exist_ok=True)
    # builder.config('spark.eventLog.enabled', 'true')
    # builder.config('spark.eventLog.dir', event_log_dir.as_uri())
    # builder.config('spark.eventLog.logBlockUpdates.enabled', 'true')
    # builder.config('spark.eventLog.longForm.enabled', 'true')
    # # builder.config('spark.eventLog.logStageExecutorMetrics', 'true')
    # builder.config('spark.sql.debug.maxToStringFields', '1024')  # 25
    # builder.config('spark.sql.maxMetadataStringLength', '4096')  # 100
    # # builder.config('spark.sql.maxPlanStringLength', '2147483632')  # 2147483632

    # # builder.config('spark.plugins', 'io.stoys.spark.StoysSparkPlugin')
    # builder.config('spark.sql.extensions', 'io.stoys.spark.StoysSparkSessionExtensions')
    # builder.config('spark.extraListeners', 'io.stoys.spark.StoysSparkListener')
    # builder.config('spark.sql.queryExecutionListeners', 'io.stoys.spark.StoysQueryExecutionListener')

    return builder.getOrCreate()
