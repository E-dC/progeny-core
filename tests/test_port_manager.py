import pytest
from progeny_core.spinner import PortManager

@pytest.mark.port_manager
@pytest.mark.parametrize(
    "port_range,expected",
    [
        (range(1, 10), range(1, 10)),
        ((1, 10), range(1, 10)),
        ([1, 10], range(1, 10)),
    ])
def test_validate_port_range(port_range, expected):
    o = PortManager.validate_port_range(port_range)
    assert isinstance(o, range)
    if not isinstance(port_range, range):
        assert o.start == port_range[0]
        assert o.stop == port_range[1]

@pytest.mark.port_manager
@pytest.mark.parametrize(
    "port_range",
    [
        range(1, 1),
        range(10, 1),
        range(10),
        (1,),
        (10, 1),
        (1, 10, 11),
        [1],
        [10, 1],
        [1, 3, 2]
    ])
def test_validate_port_range_raises(port_range):
    with pytest.raises(AssertionError):
        PortManager.validate_port_range(port_range)
