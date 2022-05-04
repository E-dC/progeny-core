import pytest
import os
import shutil
from progeny_core.spinner import ProdigyAdapter

@pytest.mark.adapter
@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({'command': 'textcat abc --loader jsonl myfile.jsonl'},
         'textcat abc --loader jsonl myfile.jsonl'),
        ({
            'recipe': 'textcat',
            'recipe_args' : ('abc', 'myfile.jsonl'), 'recipe_kwargs': {'loader': 'jsonl'}
        },
         'textcat abc myfile.jsonl --loader jsonl'),
        ({
            'recipe': 'textcat',
            'recipe_args' : ('abc', 'myfile.jsonl'), 'recipe_kwargs': {'--loader': 'jsonl'}
        },
         'textcat abc myfile.jsonl --loader jsonl'),
    ])
def test_parse_command(kwargs, expected):
    assert ProdigyAdapter.parse_command(**kwargs) == expected
