## 0.2.0 (2022-08-19)

### Feat

- **cli_serve**: make sessions accessible by identifier rather than port, improve logging, support session names
- **cli_main**: make the configurable middleware server callable from CLI, fix doc
- **cli_server**: make server configurable from a YAML file, spin instances when server is started
- **spinner**: make progeny_core maintain the process registry as a temporary SQLite db

### Fix

- **cli_serve**: fix a few bugs at app setup, wait 1 sec between each instance set up to avoid them binding to the same ports

## 0.1.0 (2022-07-23)

### Fix

- **cli_serve**: make some logging more transparent
- **cli_serve**: fix some logging issues in cli_serve, remove unused setup_logging.py file
- **cli_serve**: fix a logging error, add some logging

### Feat

- **cli**: add experimental progeny middleware and set up its CLI entrypoint

### Refactor

- **spinner**: messily switch logging to loguru for spinner

## 0.0.2 (2022-06-16)

### Fix

- **ProdigyAdapter**: change (not really fix, yet) a broken type annotation
- remove broken import of removed ProdigyController
