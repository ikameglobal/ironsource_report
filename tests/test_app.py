#!/usr/bin/env python
"""Tests for `ironsource_report` package."""
# pylint: disable=redefined-outer-name

import os
import pytest

from ironsource_report import SampleClass


@pytest.fixture
def api_key():
    api_key = os.environ.get("API_KEY", None)
    assert api_key is not None
    return api_key


def test_report(api_key):
    report = SampleClass(api_key=api_key)
    assert report is not None
