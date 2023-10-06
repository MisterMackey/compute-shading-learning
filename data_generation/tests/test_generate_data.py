import src.generate_data as app
from datetime import datetime
from pytest import fixture, approx


def test_calc_remaining_linear():
	remaining = app.calc_remaining_notional(1000, datetime(2013, 1, 1), 2.0, 'Linear', 20)
	assert approx(500) == remaining

def test_calc_remaining_annuity():
	remaining = app.calc_remaining_notional(1000, datetime(2013,1,1), 2.0, 'Annuity', 30)
	assert approx(730.6417) == remaining