"""Hand-computed values for scanner.fees.taker_fee's
ceil(0.07 * P * (1-P) * 100) cents formula. No network."""

from decimal import Decimal

import pytest

from scanner.fees import taker_fee


@pytest.mark.parametrize(
    "price,expected_cents",
    [
        ("0.04", 1),   # 0.07*.04*.96*100 = 0.2688 -> ceil 1
        ("0.03", 1),   # 0.07*.03*.97*100 = 0.2037 -> ceil 1
        ("0.30", 2),   # 0.07*.30*.70*100 = 1.47   -> ceil 2
        ("0.28", 2),   # 0.07*.28*.72*100 = 1.4112 -> ceil 2
        ("0.50", 2),   # 0.07*.50*.50*100 = 1.75   -> ceil 2, the max of the curve
        ("0.55", 2),   # 0.07*.55*.45*100 = 1.7325 -> ceil 2
        ("0.99", 1),   # 0.07*.99*.01*100 = 0.0693 -> ceil 1
    ],
)
def test_taker_fee_matches_hand_computed_cents(price, expected_cents):
    assert taker_fee(Decimal(price)) == Decimal(expected_cents) / Decimal(100)


def test_taker_fee_symmetric_around_half():
    assert taker_fee(Decimal("0.30")) == taker_fee(Decimal("0.70"))


def test_multi_contract_fee_rounds_once_after_quantity():
    # ceil(0.07 * 10 * .04 * .96 * 100) = ceil(2.688) cents
    assert taker_fee(Decimal("0.04"), Decimal(10)) == Decimal("0.03")


def test_fee_multiplier_applies_before_rounding():
    assert taker_fee(Decimal("0.50"), Decimal(10), Decimal("1.5")) == Decimal("0.27")


@pytest.mark.parametrize("price", ["0.00", "1.00", "-0.10", "1.50"])
def test_taker_fee_rejects_out_of_range_price(price):
    with pytest.raises(ValueError):
        taker_fee(Decimal(price))
