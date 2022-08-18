import prodigy
import multiprocessing
import os
import socket
import datetime
import random
import typing
from typing import Tuple, Dict, List, Iterable, Any, Optional, Union
import threading
import signal
import time
import re
import ruamel.yaml as yaml
import sqlite3

from loguru import logger
import sys
logger.add(
    sys.stdout,
    colorize=True,
    format="{time} - {name} - {level} - {message}",
    filter="spinner",
    level="INFO"
)


class PortManager(object):
    """Manage ports for ProdigyController"""

    # logger = logging.getLogger(__name__)
    # logger.propagate = False

    def __init__(
            self,
            port_range: Union[range, Tuple[int, int], List[int]]):
        """Setup a pool of usable ports."""

        self.port_range = self.validate_port_range(port_range)
        self.update_available_ports()

    def update_available_ports(self) -> None:
        self.available_ports = set([
            p for p in self.port_range
            if not self.is_port_in_use(p)
        ])

    def allocate_port(self) -> int:
        for port in self.port_range:
            if self.is_port_in_use(port):
                continue
            self.available_ports.remove(port)
            return port
        else:
            logger.error("No ports available")
            raise RuntimeError("No ports available")

    @classmethod
    def validate_port_range(
        cls,
        port_range: Union[range, Tuple[int, int], List[int]]) -> range:

        """Validate port range.

            A port range of (1, 10) will give you access to ports
            1 to 10 *non-inclusive*.

            Return a `range` object
        """

        try:
            if isinstance(port_range, (tuple, list)):
                assert len(port_range) == 2
                port_range = range(*port_range)
            else:
                assert isinstance(port_range, range)
                assert port_range.start > 0

            assert len(port_range) > 1
            return port_range

        except AssertionError:
            raise AssertionError(f'Invalid port range: {port_range}')

    @classmethod
    def is_port_in_use(cls, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) == 0



class ProdigyAdapter(object):

    # logger = logging.getLogger(__name__)
    # logger.propagate = False

    def __init__(
        self,
        recipe_dir: Optional[str] = None,
        prebaked_dir: Optional[str] = None):
        """ Load recipes, setup a registry of running processes, setup a pool
            of usable ports, and start a timed cleanup process.
        """
        self.import_recipes(recipe_dir)
        self.recipes = prodigy.core.list_recipes()
        self.prebaked = self.load_prebaked_projects(prebaked_dir)

    @classmethod
    def parse_command(cls, **kwargs):
        try:
            return kwargs['command']
        except KeyError:
            pass

        recipe = kwargs['recipe']
        recipe_args = kwargs.get('recipe_args', None)
        recipe_kwargs = kwargs.get('recipe_kwargs', None)

        assert recipe_args or recipe_kwargs

        command = f"{recipe}"
        if recipe_args:
            command = f"{command} {' '.join(recipe_args)}"
        if recipe_kwargs:
            for (k, v) in recipe_kwargs.items():
                if not k.startswith('--'):
                    k = f'--{k}'
                if isinstance(v, bool):
                    v = ''
                else:
                    v = f' {v}'
                command = f'{command} {k}{v}'

        return command

    @classmethod
    def parse_prebaked(
            cls, data: Dict[Any, Any]) -> Tuple[str, Dict[str, Any], str]:
        name = data['name']
        config = data.pop('config', {})
        command = cls.parse_command(**data)

        return (name, command, config)

    @classmethod
    def are_args_none(
            cls, data: Dict[str, Optional[str]], args: Iterable[str]) -> bool:
        return all([data.get(x) is None for x in args])

    @classmethod
    def is_unambiguous(
            cls, data: Dict[str, Optional[str]], schema: str) -> bool:

        if schema == 'prebaked':
            return cls.are_args_none(
                data, ['command', 'recipe', 'recipe_args', 'recipe_kwargs']
            )
        elif schema == 'command': # 'recipe', args, kwargs, prebaked
            return cls.are_args_none(
                data, ['recipe', 'recipe_args', 'recipe_kwargs']
            )
        elif schema == 'recipe': # prebaked, command
            return cls.are_args_none(
                data, ['command']
            )
        return False

    @classmethod
    def ensure_unambiguity(
            cls,
            data: Dict[str, Optional[str]],
            is_prebaked_data: bool
    ) -> Optional[Dict[str, Optional[str]]]:

        if is_prebaked_data:
            try:
                assert 'name' in data
            except AssertionError:
                logger.warning('Not prebaked data: missing a `name` key')
                return None

        try:
            if 'command' in data:
                assert cls.is_unambiguous(data, 'command')
            elif 'recipe' in data:
                assert cls.is_unambiguous(data, 'recipe')
            else:
                raise AssertionError
        except AssertionError:
            logger.warning('Not a valid prebaked project file')
            return None

        return data

    @classmethod
    def try_load_prebaked(cls, filepath: str) -> Optional[Dict[Any, Any]]:
        if not filepath.endswith('.yml'):
            return None

        logger.info(f'Attempting to load {filepath}')
        try:
            with open(filepath, 'r') as f:
                data = yaml.safe_load(f)
        except yaml.error.YAMLError:
            logger.warning('Not a valid YAML file')
            return None

        return cls.ensure_unambiguity(data, True)

    @classmethod
    def load_prebaked_projects(
            cls, prebaked_dir: Optional[str]) -> Dict[Any, Any]:

        o = {}
        if prebaked_dir:
            logger.info(
                f"Looking for prebaked projects in {os.path.abspath(prebaked_dir)}")
            for root, dirs, files in os.walk(prebaked_dir):
                for filename in files:
                    data = cls.try_load_prebaked(os.path.join(root, filename))
                    if data:
                        name, command, config = cls.parse_prebaked(data)
                        o[name] = (command, config)

        found = set(o.keys())
        suffix = f': {sorted(found)}' if found else ''
        logger.info(
            f"{len(found)} prebaked projects found{suffix}")
        return o

    @classmethod
    def try_import_recipe(cls, filepath: str) -> None:
        logger.info(f"Loading recipes from {filepath}")
        try:
            return prodigy.core.import_code(filepath)
        except:
            raise

    @classmethod
    def is_recipe(
            cls,
            filename: str,
            exclude_pattern: Optional[str] = None) -> bool:

        if not filename.endswith('.py'):
            return False

        if (exclude_pattern is None
            or not re.search(pattern=exclude_pattern, string=filename)):
            return True

        return False

    @classmethod
    def import_recipes(
            cls,
            recipe_dir: Optional[str],
            exclude_pattern: Optional[str] = '^(_|loader).*') -> set:
        """ Make our custom recipes visible to Prodigy, so that we can call
            them by recipe name, and without having to give the file name as
            an argument when spinning an instance.

            Example: we'll be able to call
                prodigy my.recipe arg1 arg2
            instead of
                prodigy my.recipe arg1 arg2 -F filename_containing_my_recipe.py

            Args:
                recipe_dir (str or None): Directory to load recipes from
                exclude_pattern (str or None): Regex pattern matching files
                    to ignore in `recipe_dir`
        """

        starting_recipes = set(prodigy.core.list_recipes())
        if recipe_dir:
            logger.info(
                f"Looking for recipes in {os.path.abspath(recipe_dir)}")
            for root, dirs, files in os.walk(recipe_dir):
                for filename in files:
                    if cls.is_recipe(filename, exclude_pattern):
                        cls.try_import_recipe(os.path.join(root, filename))

        available_recipes = set(prodigy.core.list_recipes())
        found = available_recipes - starting_recipes
        suffix = f': {sorted(found)}' if found else ''
        logger.info(
            f"{len(found)} additional recipes found{suffix}")

        return available_recipes

    @classmethod
    def prodigy_serve(
        cls,
        command: str,
        config: Dict[str, Any],
        env: Optional[Dict[str, str]] = None,
        session_name: Optional[str] = None) -> None:
        """ Start serving a Prodigy instance"""

        if env and session_name:
            env['PRODIGY_ALLOWED_SESSIONS'] = session_name
            os.environ = env
        prodigy.serve(command, **config)


class ProcessRegistry(object):

    def __init__(
            self,
            port_manager: PortManager,
            scheduled_cleaning_interval: Optional[int],
            scheduled_cleaning_timeout: Optional[int]):

        self.port_manager = port_manager
        self.db_name = "progeny-processes.db"
        self.attach_to_db()

        self.scheduled_stopper = None
        self.setup_scheduled_cleaning(
            scheduled_cleaning_interval, scheduled_cleaning_timeout)


    def attach_to_db(self):
        self.connection = sqlite3.connect(self.db_name)
        self.connection.row_factory = sqlite3.Row
        stmt = (
            "CREATE TABLE IF NOT EXISTS Process("
            "    identifier text PRIMARY KEY, "
            "    session_name text, "
            "    port int NOT NULL UNIQUE, "
            "    pid int NOT NULL UNIQUE, "
            "    expiry_timestamp int NOT NULL)"
        )

        cur = self.connection.cursor()
        cur.execute(stmt)
        self.connection.commit()
        return None

    def setup_scheduled_cleaning(
        self,
        interval: Optional[int],
        timeout: Optional[int]):

        if interval and timeout:
            signal.signal(signal.SIGTERM, self.catch_signal)
            signal.signal(signal.SIGINT, self.catch_signal)
            signal.signal(signal.SIGQUIT, self.catch_signal)

            self.scheduled_stopper = threading.Event()
            self.scheduled_cleaning(interval, timeout)

    def __del__(self):
        """ Make sure to stop the cleaning process when destroying instance"""
        if self.scheduled_stopper:
            self.scheduled_stopper.set()
            self.current_timer.cancel()

        self.cleanup_running_processes(allow_unsafe=True)
        os.remove(self.db_name)

    def catch_signal(
        self, signum: int, frame: typing.types.FrameType):
        """ When catching SIGTERM, SIGINT or SIGQUIT, stop any running
            cleaning process before exiting
        """
        if self.scheduled_stopper:
            logger.info('Stopping timer')
            self.scheduled_stopper.set()
            self.current_timer.cancel()

        self.cleanup_running_processes(allow_unsafe=True)
        os.remove(self.db_name)
        exit(1)

    def scheduled_cleaning(
        self,
        interval: int,
        timeout: int):
        """ Cleanup any running process, then set up a new timer at the end of
            which interval we call `scheduled_cleaning` method again.
        """

        logger.debug('Cleaning processes')
        self.cleanup_running_processes(timeout=timeout)

        if not self.scheduled_stopper.is_set():
            self.current_timer = threading.Timer(
                interval,
                self.scheduled_cleaning,
                (interval, timeout))
            self.current_timer.start()

    def register_process(
        self,
        identifier: str,
        session_name: Optional[str],
        port: int,
        process: Union[multiprocessing.Process, int],
        expiry_timestamp: int = 8_000_000_000) -> None:
        """ Add a process and its metadata to the registry."""

        if isinstance(process, multiprocessing.Process):
            assert process.is_alive()
            pid = process.pid
        else:
            pid = process

        stmt = (
            "INSERT INTO Process (identifier, session_name, port, pid, expiry_timestamp) "
            "VALUES (?, ?, ?, ?, ?)"
        )

        cur = self.connection.cursor()
        cur.execute(
            stmt,
            (identifier, session_name, port, pid, round(expiry_timestamp))
        )
        self.connection.commit()
        self.port_manager.update_available_ports()

        return None

    def conds_maker(self, operator: str = "OR", **kwargs) -> Tuple[str, Tuple[str]]:
        """Build a WHERE SQL prepared statement out of **kwargs
                - registered identifier matches `identifier`
                - registered session_name matches `session_name`
                - registered port matches `port`
                - registered pid matches `pid`
                - registered process is older than `older_than` (in seconds)

            To ignore arguments, omit them or set as None.
        """
        assert operator in ("OR", "AND")
        conds = []
        values = []
        for k, v in kwargs.items():
            if kwargs[k]:
                if k == "older_than":
                    conds.append(f"{k} > ?")
                elif k in ["identifier", "session_name", "pid", "port"]:
                    conds.append(f"{k} = ?")
                values.append(v)

        conds_stmt = f" {operator} ".join(conds)
        if conds_stmt:
            conds_stmt = f"WHERE {conds_stmt}"
        values = tuple(values)

        return (conds_stmt, values)

    def is_safe(self, conds_stmt: str, allow_unsafe: bool) -> bool:
        if conds_stmt:
            return True

        if allow_unsafe:
            return True

        logger.warning(
            "All records selected (no conditions were set), "
            "but `allow_unsafe` is False: not selecting or deleting anything"
        )
        return False


    def find_process_from_registry(
        self,
        allow_unsafe: bool = False,
        **kwargs
    ) -> List[Dict[str, Union[str, int]]]:
        """ SELECT one or more processes from the registry.
            See `self.conds_maker` for details on how selection is made
        """

        conds_stmt, values = self.conds_maker(**kwargs)
        if not self.is_safe(conds_stmt, allow_unsafe):
            return []

        stmt = f"SELECT * FROM Process {conds_stmt}"

        cur = self.connection.cursor()
        res = cur.execute(stmt, values).fetchall()

        return [dict(r) for r in res]

    def delete_process_from_registry(
        self,
        allow_unsafe: bool = False,
        **kwargs
    ) -> None:
        """ DELETE one or more processes from the registry.
            See `self.conds_maker` for details on how selection is made
        """

        conds_stmt, values = self.conds_maker(**kwargs)
        if not self.is_safe(conds_stmt, allow_unsafe):
            return None

        stmt = f"DELETE FROM Process {conds_stmt}"

        cur = self.connection.cursor()
        cur.execute(stmt, values)
        self.connection.commit()

        return None

    def terminate_target_process(self, pid: int) -> None:
        os.kill(pid, signal.SIGTERM)
        time.sleep(1)
        logger.info(f'Terminated process {pid}')
        return None

    def cleanup_running_processes(self, allow_unsafe=False, **kwargs) -> int:
        """ Terminate one or more processes and clean them up from the registry.
            See `self.conds_maker` for details on how selection is made
        """

        records = self.find_process_from_registry(allow_unsafe=allow_unsafe, **kwargs)
        for record in records:
            logger.info(f'Cleaning up process: {record}...')
            self.terminate_target_process(record["pid"])

        self.delete_process_from_registry(allow_unsafe=allow_unsafe, **kwargs)
        self.port_manager.update_available_ports()

        return len(records)

    def already_has_instance(self, identifier: str) -> bool:
        if self.find_process_from_registry(identifier=identifier):
            logger.error(
                f"{identifier} already started a Prodigy instance")
            return True
        return False

    @classmethod
    def get_time(cls) -> datetime.datetime:
        return datetime.datetime.now()

class Progeny(object):
    """Spin, monitor and destroy Prodigy instances."""

    # logger = logging.getLogger(__name__)
    # logger.propagate = False

    def __init__(
            self,
            recipe_dir: Optional[str] = None,
            prebaked_dir: Optional[str] = None,
            port_range: Union[range, Tuple[int, int], List[int]] = (8080, 8090),
            scheduled_cleaning_interval: Optional[int] = None,
            scheduled_cleaning_timeout: Optional[int] = None):
        """ Load recipes, setup a registry of running processes, setup a pool
            of usable ports, and start a timed cleanup process.
        """

        self.port_manager = PortManager(port_range)
        self.adapter = ProdigyAdapter(recipe_dir, prebaked_dir)
        self.registry = ProcessRegistry(
            self.port_manager,
            scheduled_cleaning_interval,
            scheduled_cleaning_timeout
        )
        self.cleanup_running_processes = self.registry.cleanup_running_processes

    @classmethod
    def build_session_name(cls, identifier: str, uniquify: bool = True) -> str:
        """ Create a random session name to make the instance url unpredictable
            even if we know the identifier. This is because the Prodigy instance
            itself is not password-protected, and so an attacker with the url
            would be able to access it.
        """
        if uniquify:
            return f'{identifier}-{hash(random.random())}'
        return identifier


    def get_process(
            self,
            command: str,
            config: Dict[str, Any],
            session_name: str) -> multiprocessing.Process:
        """ Prepare a new process on which we can start a Prodigy instance"""

        env = os.environ.copy()
        p = multiprocessing.Process(
                target=self.adapter.prodigy_serve,
                name=f'progeny-{session_name}',
                args=(command, config, env, session_name))

        return p

    def get_command_and_config(
            self,
            command: Optional[str] = None,
            prebaked: Optional[str] = None,
            recipe: Optional[str] = None,
            recipe_args: Optional[Union[Tuple[str], List[str]]] = None,
            recipe_kwargs: Optional[Dict[Any, Any]] = None,
            config: Optional[Dict[Any, Any]] = None) -> Tuple[str, Dict[Any, Any]]:

        def is_unambiguous(*args):
            return all([x is None for x in args])

        if not config:
            config = {}

        if command:
            assert is_unambiguous(prebaked, recipe, recipe_args, recipe_kwargs)
        elif prebaked:
            assert is_unambiguous(command, recipe, recipe_args, recipe_kwargs)
            command, bconfig = self.adapter.prebaked[prebaked]
            config = {**bconfig, **config}
        elif recipe:
            assert is_unambiguous(prebaked, command)
            command = self.adapter.parse_command(
                recipe=recipe,
                recipe_args=recipe_args,
                recipe_kwargs=recipe_kwargs
            )

        return (command, config)

    def spin(
            self,
            identifier: str,
            command: Optional[str] = None,
            prebaked: Optional[str] = None,
            recipe: Optional[str] = None,
            recipe_args: Optional[Union[Tuple[str], List[str]]] = None,
            recipe_kwargs: Optional[Dict[Any, Any]] = None,
            config: Optional[Dict[Any, Any]] = None,
            uniquify: bool = False) -> Tuple[int, str]:
            # """ Check we can start a new instance, construct the instance arguments
            #     and configuration, prepare the process, start it, and update the
            #     registry appropriately.
            #
            #     `recipe_args`'s first element should pretty much always be the
            #     dataset name (in which we store the annotations)
            #
            #     `loader` and `loader_args` are useful when you want to use a default
            #     Prodigy recipe and load the data to annotate straight from a flat
            #     file (see `construct_loader_command`)
            # """

        command, config = self.get_command_and_config(
            command, prebaked, recipe, recipe_args, recipe_kwargs, config)

        if self.registry.already_has_instance(identifier):
            raise ValueError(f"{identifier} already has a process running")
        port = self.port_manager.allocate_port()
        session_name = self.build_session_name(identifier, uniquify=uniquify)

        config = {**config, 'port': port}

        process = self.get_process(command, config, session_name)
        logger.info(f'Process created: ({identifier}, {session_name}, {port}) ')
        process.start()

        self.registry.register_process(
            identifier,
            session_name,
            port,
            process
        )


        return (port, session_name)
        # return f'127.0.0.1:{port}/?session={session_name}'
