#!/usr/bin/env python
"""Tests for `ironsource_report` package."""
# pylint: disable=redefined-outer-name

import os
import pytest

from ironsource_report import AdRevenueMeasurements


@pytest.fixture
def api_key():
    api_key = os.environ.get("API_KEY", {"key": "xxx"})
    assert api_key is not None
    return api_key


def test_report(api_key):
    # reporter = AdRevenueMeasurements(api_credential=api_key)
    assert True
