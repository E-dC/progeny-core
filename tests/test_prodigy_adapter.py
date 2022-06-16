import pytest
from progeny_core.spinner import ProdigyAdapter

valid_commands_kwargs = [
    {'command': 'textcat abc --loader jsonl myfile.jsonl'},
    {'recipe': 'textcat',
     'recipe_args' : ('abc', 'myfile.jsonl'),
     'recipe_kwargs': {'loader': 'jsonl'}
    },
    {'recipe': 'textcat',
     'recipe_args' : ('abc', 'myfile.jsonl'),
     'recipe_kwargs': {'--loader': 'jsonl'}
    }
]
valid_commands_none_args = [
    ['recipe', 'recipe_args', 'recipe_kwargs'],
    ['command'], ['command']
]
valid_commands_schema_names = ['command', 'recipe', 'recipe']

expected_parsed_commands = [
    'textcat abc --loader jsonl myfile.jsonl',
    'textcat abc myfile.jsonl --loader jsonl',
    'textcat abc myfile.jsonl --loader jsonl'
]

# invalid_commands_kwargs = [
#     {'command': 'textcat abc --loader jsonl myfile.jsonl',
#      'recipe': 'xyz'},
#     {'recipe': 'textcat',
#      'command': 'blabla',
#      'recipe_args' : ('abc', 'myfile.jsonl'),
#      'recipe_kwargs': {'loader': 'jsonl'}
#     },
#     {'recipe': 'textcat'}
# ]
# invalid_commands_none_args = [
#     ['recipe', 'recipe_args', 'recipe_kwargs'],
#     ['command'], ['command']
# ]


@pytest.mark.adapter
@pytest.mark.parametrize(
    "kwargs,expected",
    [(data, expected)
     for data, expected
     in zip(valid_commands_kwargs, expected_parsed_commands)])
def test_parse_command(kwargs, expected):
    assert ProdigyAdapter.parse_command(**kwargs) == expected


@pytest.mark.adapter
@pytest.mark.parametrize(
    "data,none_args,expected",
    [(data, args, True)
     for data, args
     in zip(valid_commands_kwargs, valid_commands_none_args)])
def test_are_args_none(data, none_args, expected):
    assert (
        ProdigyAdapter.are_args_none(data, none_args) == expected
    )

@pytest.mark.adapter
@pytest.mark.parametrize(
    "data,schema,expected",
    [(data, args, True)
     for data, args
     in zip(valid_commands_kwargs, valid_commands_schema_names)])
def test_is_unambiguous(data, schema, expected):
    assert (
        ProdigyAdapter.is_unambiguous(data, schema) == expected
    )


@pytest.mark.adapter
@pytest.mark.parametrize(
    "data",
    valid_commands_kwargs)
def test_ensure_unambiguity(data):
    assert ProdigyAdapter.ensure_unambiguity(data, False) is not None
