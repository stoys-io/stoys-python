import pandas as pd
import pytest
from pyspark.sql import SparkSession


def test_stoys_init():
    df = pd.DataFrame([{"k": "1", "v": 42.0, "e": "foo"}])
    if SparkSession.getActiveSession() is not None:
        print("This test only make sense to run standalone (or first). Ignoring it now")
    else:
        with pytest.raises(AttributeError):
            df.stoys.dp()
        import stoys

        assert SparkSession.getActiveSession() is None
        with pytest.raises(AttributeError):
            df.stoys.dp()
        stoys.init()
        assert SparkSession.getActiveSession() is None
        assert df.stoys.dp().table.rows == 1
        assert SparkSession.getActiveSession() is not None
