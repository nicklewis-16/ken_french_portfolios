import pandas as pd
from pandas.testing import assert_frame_equal

import calc_CRSP_indices
import load_CRSP_stock

import config
DATA_DIR = config.DATA_DIR

def test_calc_value_idx():
    """
    Consider the hypothetical example defined in the DataFrame `input`

    Now, calculating the value-weighted returns by hand, we should see the following:

    time=1:
      prev mktcap permno #1 is 100 * 1 = 100
      prev mktcap permno #2 is 200 * 2 = 400
      curr returns permno #1 is -0.5
      curr returns permno #2 is 0.1
      So value-weighted return is (100 * -0.5 + 400 * 0.1) / (100 + 400) = -0.02
    time=2:
      prev mktcap permno #1 is 100 * 0.5 = 50
      prev mktcap permno #2 is 200 * 2.2 = 440
      curr returns permno #1 is 1
      curr returns permno #2 is 0.1
      So value-weighted return is (50 * 1 + 440 * 0.1) / (50 + 440) = 0.1918...
    and total market cap should 500, 0.5 * 100 + 2.2 * 200 = 490, 100 + 2.42 * 200 = 584.0
    """
    input = pd.DataFrame(
        data={
            "permno": [1, 1, 1, 2, 2, 2],
            "date": pd.to_datetime(
                [
                    "2020-01-01",
                    "2020-02-01",
                    "2020-03-01",
                    "2020-01-01",
                    "2020-02-01",
                    "2020-03-01",
                ]
            ),
            "altprc": [1, 0.5, 1, 2, 2.2, 2.42],
            "ret": [0, -0.5, 1, 0, 0.1, 0.1],
            "retx": [0, -0.5, 1, 0, 0.1, 0.1],
            "shrout": [100, 100, 100, 200, 200, 200],
        }
    )

    expected_output = pd.DataFrame(
        data={
            "vwretd": [-0.02, 0.1918],
            "vwretx": [-0.02, 0.1918],
            "totval": [490.0, 584.0],
        },
        index=pd.to_datetime(["2020-02-01", "2020-03-01"]),
    )
    expected_output.index.name = "date"

    assert_frame_equal(
        calc_CRSP_indices.calc_CRSP_value_weighted_index(input).round(4),
        expected_output.round(4),
    )


def test_calc_equal_idx():
    input = pd.DataFrame(
        data={
            "permno": [1, 1, 1, 2, 2, 2],
            "date": pd.to_datetime(
                [
                    "2020-01-01",
                    "2020-02-01",
                    "2020-03-01",
                    "2020-01-01",
                    "2020-02-01",
                    "2020-03-01",
                ]
            ),
            "altprc": [1, 0.5, 1, 2, 2.2, 2.42],
            "ret": [0, -0.5, 1, 0, 0.1, 0.1],
            "retx": [0, -0.5, 1, 0, 0.1, 0.1],
            "shrout": [100, 100, 100, 200, 200, 200],
        }
    )

    # Expected output:
    # time=0:
    #   returns are 0 and 0
    #   So equal-weighted return is (0 + 0) / 2 = 0
    # time=1:
    #   returns are -0.5 and 0.1
    #   So equal-weighted return is (-0.5 + 0.1) / 2 = -0.2
    # time=2:
    #   returns are 1 and 0.1
    #   So equal-weighted return is (1 + 0.1) / 2 = 0.55

    expected_output = pd.DataFrame(
        data={
            "ewretd": [0, -0.2, 0.55],
            "ewretx": [0, -0.2, 0.55],
            "totcnt": [2, 2, 2],
        },
        index=pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]),
    )
    expected_output.index.name = "date"

    assert_frame_equal(
        calc_CRSP_indices.calc_equal_weighted_index(input).round(4),
        expected_output.round(4),
    )


def test_calc_CRSP_indices_merge():
    """
    Same as before:
        total market cap should 500, 0.5 * 100 + 2.2 * 200 = 490, 100 + 2.42 * 200 = 584.0
    """
    input = pd.DataFrame(
        data={
            "permno": [1, 1, 1, 2, 2, 2],
            "date": pd.to_datetime(
                [
                    "2020-01-01",
                    "2020-02-01",
                    "2020-03-01",
                    "2020-01-01",
                    "2020-02-01",
                    "2020-03-01",
                ]
            ),
            "altprc": [1, 0.5, 1, 2, 2.2, 2.42],
            "ret": [0, -0.5, 1, 0, 0.1, 0.1],
            "retx": [0, -0.5, 1, 0, 0.1, 0.1],
            "shrout": [100, 100, 100, 200, 200, 200],
        }
    )

    idx_input = pd.DataFrame(
        data={
            "vwretd": [0, -0.03, 0.2],
            "vwretx": [0, -0.03, 0.2],
            "totval": [500.0, 490.0, 584.0],
            "ewretd": [0, -0.2, 0.5],
            "ewretx": [0, -0.2, 0.5],
            "totcnt": [0, 2, 2],
        },
        index=pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]),
    )
    idx_input.index.name = "date"

    # This just combines the logic from the two previous tests.

    expected_output = pd.DataFrame(
        data={
            "vwretd": [-0.03, 0.2],
            "vwretx": [-0.03, 0.2],
            "totval": [490.,584.],
            "ewretd": [-0.2, 0.5],
            "ewretx": [-0.2, 0.5],
            "totcnt": [2, 2],
            "vwretd_manual": [-0.02, 0.1918],
            "vwretx_manual": [-0.02, 0.1918],
            "totval_manual": [490.,584.],
            "ewretd_manual": [-0.2, 0.55],
            "ewretx_manual": [-0.2, 0.55],
            "totcnt_manual": [2, 2],
        },
        index=pd.to_datetime(["2020-02-01", "2020-03-01"]),
    )
    expected_output.index.name = "date"

    assert_frame_equal(
        calc_CRSP_indices.calc_CRSP_indices_merge(input, idx_input).round(4),
        expected_output.round(4),
    )
