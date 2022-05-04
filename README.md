# Progeny-core

The package is still very much in development.


## Table of contents
* [General info](#general-info)
* [Setup](#setup)
* [Running Progeny-core](#running-progeny-core)
* [Prebaked projects](#prebaked-projects)


## General info  

Progeny-core is a way to start, monitor, and destroy Prodigy instances.
The goal is to make it easier to start Prodigy instances for multiple, non-technical annotators.

The `core` part of `progeny-core` is because of some unreleased (so far) whose name is also `progeny`...


## Setup

### Dependencies
 - `prodigy >= 1.10.8` : this needs to be installed from a wheel (`prodigy` isn't open-source). This is the only thing you need to work with only `ProdigyController`


### Install

```bash
# Make a virtual environment if you don't want to cry
git clone https://github.com/E-dC/progeny-core.git # or git@github.com:Ed-C/progeny-core.git
cd progeny-core
# Install dependencies with pip....
```

## Running Progeny-core

The important bit is the `Progeny` class

```python
from progeny_core import Progeny

progenitor = Progeny(
    recipe_dir='path/to/my/special/recipes/if/I/have/any',
    prebaked_dir='path/to/my/prebaked/projects/if/I/have/any',

    # Ports I want to use to spin instances
    port_range=(8080, 8090),

    # How often (in seconds) we check if an instance has timed out
    # (defaults to None: don't check)
    scheduled_cleaning_interval=60,

    # How long (in seconds) a process is allowed to run before timing out
    # (defaults to None: don't check)
    scheduled_cleaning_timeout=3600
)

port_used, session_name = progenitor.spin(
    username='archibald',
    command='textcat.manual my_output_dataset.jsonl path/to/my/amazing/dataset.jsonl --loader jsonl')
# or (assuming the prebaked project was loaded when instantiating `progenitor`):
# port_used, session_name = progenitor.spin(
#     username='archibald',
#     prebaked='my_amazing_prebaked_project')
# or:
# port_used, session_name = progenitor.spin(
#     username='archibald',
#     recipe='textcat.manual',
#     recipe_args=('my_output_dataset', 'path/to/my/amazing/dataset.jsonl'),
#     recipe_kwargs={'loader': 'jsonl'})


# The URL of the instance to access uses NAMED SESSIONS:
# http://0.0.0.0:8080/?session=archibald
# NOT http://0.0.0.0:8080/

# Annotate stuff for a while


progenitor.cleanup_running_processes(port=port)
# or:
# progenitor.cleanup_running_processes(session_name=session_name)
# or:
# progenitor.cleanup_running_processes(username='archibald')
# or:
# progenitor.cleanup_running_processes(timeout=60)# Terminate processes older than `timeout` seconds
```

The method signature:
```python
Signature:
Progeny.spin(
    self,
    username: str,
    command: Union[str, NoneType] = None,
    prebaked: Union[str, NoneType] = None,
    recipe: Union[str, NoneType] = None,
    recipe_args: Union[Tuple[str], List[str], NoneType] = None,
    recipe_kwargs: Union[Dict[Any, Any], NoneType] = None,
    config: Union[Dict[Any, Any], NoneType] = None,
    uniquify: bool = False,
) -> Tuple[int, str]
```

## Prebaked projects
Prebaked projects are a simple way of storing your annotation configurations
in YAML files and just refer to their name when calling `Progeny.spin`
An example can be found in `examples/example_baked.yml`, and below.

```yaml
# Compulsory
# Name we use to refer to the project when spinning a new instance
name: 'prebaked_project'

# Optional
# Override prodigy.json config values
# (except "port", which is set by Progeny: that's the whole point of the package)
config:
    feed_overlap: True
    auto_count_stream: True
    # ...

# Here you MUST pick EITHER the
# "recipe, recipe_args, recipe_kwargs" spec
# OR the
# "command" spec

# With the "recipe, recipe_args, recipe_kwargs" spec, `recipe` must be set and
# at least one of `recipe_args` or `recipe_kwargs` must be set.

# Name of the recipe
recipe: 'textcat.manual'
# Positional arguments of the recipe
recipe_args:
    - 'dataset_name'
    - 'myfile.jsonl'
# Keyword arguments of the recipe
recipe_kwargs:
    loader: 'jsonl'
    lang: 'en'
    split_by_lang: True

# OR

# With the "command" spec, `command` must be set
command: 'textcat.manual dataset_name myfile.jsonl --loader jsonl --lang en --split_by_lang'
```
