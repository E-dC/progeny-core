# Progeny

The package is still very much in development.


## Table of contents
* [General info](#general-info)
* [Setup](#setup)
* [Running Progeny](#running-progeny)


## General info  

Progeny-core is a way to start, monitor, and destroy Prodigy instances.
The goal is to make it easier to start Prodigy instances for multiple, non-technical annotators.

The `core` part of `progeny-core` is because of some unreleased (so far) whose name is also `progeny`...


## Setup

### Dependencies
 - `prodigy >= 1.10.8` : this needs to be installed from a wheel (`prodigy` isn't open-source). This is the only thing you need to work with only `ProdigyController`


### Install

```bash
git clone https://github.com/E-dC/progeny-core.git # or git@github.com:Ed-C/progeny-core.git
cd progeny
# Make a virtual environment if you don't want to cry
# Install dependencies with pip....
```

### Running Progeny

The important bit is the `ProdigyController` class

```python
from progeny.spinner import ProdigyController

progenitor = ProdigyController(
    recipe_dir='path/to/my/special/recipes/if/I/have/any',
    port_range=(8080, 8090)) # Ports I want to use to spin instances

port_used, session_name = progenitor.spin(
    username='archibald',
    recipe='textcat.manual',
    recipe_args=('my_output_dataset',),
    loader='jsonl',
    loader_args=('path/to/my/amazing/dataset.jsonl',))

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

```python
help(progenitor.spin)
Signature:
ProdigyController.spin(
    self,
    username: str,
    recipe: str,
    recipe_args: Tuple[str],
    recipe_kwargs: Union[Dict[Any, Any], NoneType] = None,
    loader: Union[str, NoneType] = None,
    loader_args: Union[Tuple[str], NoneType] = None,
) -> Tuple[int, str]

```

