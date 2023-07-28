import pytest

from utils import (
    MDField,
    map_dictionary,
)

RANDOM_VALUE = [None, "", 0, False, "some_string", 42, -0.13]


@pytest.mark.parametrize("v", RANDOM_VALUE)
def test_const_mapping(v):
    map = {"ok1": v}

    expected_map = {"ok1": v}

    assert map_dictionary({}, map) == expected_map


@pytest.mark.parametrize("v", RANDOM_VALUE)
def test_field_mapping(v):
    in_dict = {"ik1": v}

    map = {"ok1": MDField(["ik1"])}

    expected_map = {"ok1": v}

    assert map_dictionary(in_dict, map) == expected_map


@pytest.mark.parametrize("v", RANDOM_VALUE)
def test_missing_key(v):
    in_dict = {"ik1": v}

    map = {"ok1": MDField(["ik_wrong"])}

    expected_map = {"ok1": None}

    assert map_dictionary(in_dict, map) == expected_map


def test_field_path_mapping():
    in_dict = {"ik1": {"ik11": "val11"}}

    map = {"ok1": MDField(["ik1", "ik11"])}

    expected_map = {"ok1": "val11"}

    assert map_dictionary(in_dict, map) == expected_map


def test_field_post_processing():
    in_dict = {"ik1": "val1"}

    map = {"ok1": MDField(["ik1"], post_process=str.upper)}

    expected_map = {"ok1": "VAL1"}

    assert map_dictionary(in_dict, map) == expected_map


def test_field_required_none():
    in_dict = {"ik1": None}

    map = {"ok1": MDField(["ik1"], is_required=True)}

    with pytest.raises(Exception):
        map_dictionary(in_dict, map)


def test_field_required_absent():
    map = {"ok1": MDField(["ik1"], is_required=True)}

    with pytest.raises(Exception):
        map_dictionary({}, map)


def test_field_list():
    in_dict = {
        "ik1": [
            {
                "ikl1": "v11",
                "ikl2": "v12",
            },
            {
                "ikl1": "v21",
                "ikl2": "v22",
            },
        ],
    }

    map = {"ok1": ["ik1", {"okl12": MDField(["ikl2"])}]}

    expected_map = {"ok1": [{"okl12": "v12"}, {"okl12": "v22"}]}

    assert map_dictionary(in_dict, map) == expected_map


def test_field_list_with_upper_level():
    in_dict = {
        "ik1": [
            {
                "ikl1": "v11",
                "ikl2": "v12",
            },
            {
                "ikl1": "v21",
                "ikl2": "v22",
            },
        ],
        "ik2": 42,
    }

    map = {
        "ok1": [
            "ik1",
            {
                "okl12": MDField(["ikl2"]),
                "okl13": MDField(["ik2"]),
            },
        ]
    }

    expected_map = {
        "ok1": [
            {
                "okl12": "v12",
                "okl13": 42,
            },
            {
                "okl12": "v22",
                "okl13": 42,
            },
        ]
    }

    assert map_dictionary(in_dict, map) == expected_map
