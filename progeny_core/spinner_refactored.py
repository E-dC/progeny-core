import prodigy
import multiprocessing
import os
import socket
import datetime
import random
import typing
from typing import Tuple, Dict, List, Any, Optional, Union
import threading
import signal
import time
import re
import ruamel.yaml as yaml

try:
    from .setup_logging import logging
except ImportError:
    from setup_logging import logging


class PortManager(object):
    """Manage ports for ProdigyController"""

    logger = logging.getLogger(__name__)

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
            self.logger.error("No ports available")
            raise RuntimeError("No ports available")

    @classmethod
    def validate_port_range(
        cls,
        port_range: Union[range, Tuple[int, int], List[int]]) -> range:

        try:
            if isinstance(port_range, (tuple, list)):
                assert len(port_range) == 2
                return range(*port_range)
            else:
                assert isinstance(port_range, range)
                return port_range

        except AssertionError:
            raise AssertionError(f'Invalid port range: {port_range}')

    @classmethod
    def is_port_in_use(cls, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) == 0



class ProdigyAdapter(object):

    logger = logging.getLogger(__name__)

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
    def parse_prebaked(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        name = data['name']
        config = data.pop('config', {})
        command = cls.parse_command(**data)

        return (name, command, config)

    @classmethod
    def try_load_prebaked(cls, filepath: str) -> Optional[Dict[Any, Any]]:

        def is_unambiguous(*args):
            return all([data.get(x) is None for x in args])

        try:
            with open(filepath, 'r') as f:
                data = yaml.safe_load(f)
        except yaml.error.YAMLError:
            cls.logger.warning(f'Not a YAML file: {filepath}')
            return None

        try:
            assert 'name' in data
            if 'command' in data:
                assert is_unambiguous('recipe', 'recipe_args', 'recipe_kwargs')
            if 'recipe' in data:
                assert is_unambiguous('command')
                assert (
                    data.get('recipe_args') is not None
                    or data.get('recipe_kwargs') is not None
                )
        except AssertionError:
            cls.logger.warning(f'Not a prebaked project file: {filepath}')
            return None
        return data

    @classmethod
    def load_prebaked_projects(
            cls, prebaked_dir: Optional[str]) -> Dict[Any, Any]:

        o = {}
        if prebaked_dir:
            cls.logger.info(
                f"Looking for prebaked projects in {os.path.abspath(prebaked_dir)}")
            for root, dirs, files in os.walk(prebaked_dir):
                for filename in files:
                    data = cls.try_load_prebaked(os.path.join(root, filename))
                    if data:
                        name, command, config = cls.parse_prebaked(data)
                        o[name] = (command, config)

        found = set(o.keys())
        suffix = f': {sorted(found)}' if found else ''
        cls.logger.info(
            f"{len(found)} prebaked projects found{suffix}")
        return o

    @classmethod
    def try_import_recipe(cls, filepath: str) -> None:
        cls.logger.info(f"Loading recipes from {filepath}")
        try:
            return prodigy.core.import_code(filepath)
        except:
            raise

    @classmethod
    def is_recipe(
            cls,
            filename: str,
            ignored_pattern: Optional[str] = None) -> bool:

        if not filename.endswith('.py'):
            return False

        if (ignored_pattern is None
            or not re.search(pattern=ignored_pattern, string=filename)):
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
            cls.logger.info(
                f"Looking for recipes in {os.path.abspath(recipe_dir)}")
            for root, dirs, files in os.walk(recipe_dir):
                for filename in files:
                    if cls.is_recipe(filename):
                        cls.try_import_recipe(os.path.join(root, filename))

        available_recipes = set(prodigy.core.list_recipes())
        found = available_recipes - starting_recipes
        suffix = f': {sorted(found)}' if found else ''
        cls.logger.info(
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

    logger = logging.getLogger(__name__)

    def __init__(
            self,
            port_manager: PortManager,
            scheduled_cleaning_interval: Optional[int],
            scheduled_cleaning_timeout: Optional[int]):

        self.port_manager = port_manager
        self.running_processes = []

        self.scheduled_stopper = None
        self.setup_scheduled_cleaning(
            scheduled_cleaning_interval, scheduled_cleaning_timeout)


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

        self.cleanup_running_processes(timeout=0.001)

    def catch_signal(
        self, signum: int, frame: typing.types.FrameType):
        """ When catching SIGTERM, SIGINT or SIGQUIT, stop any running
            cleaning process before exiting
        """
        if self.scheduled_stopper:
            self.logger.info('Stopping timer')
            self.scheduled_stopper.set()
            self.current_timer.cancel()
            exit(1)

    def scheduled_cleaning(
        self,
        interval: int,
        timeout: int):
        """ Cleanup any running process, then set up a new timer at the end of
            which interval we call `scheduled_cleaning` method again.
        """

        self.logger.debug('Cleaning processes')
        self.cleanup_running_processes(timeout=timeout)

        if not self.scheduled_stopper.is_set():
            self.current_timer = threading.Timer(
                interval,
                self.scheduled_cleaning,
                (interval, timeout))
            self.current_timer.start()

    def register_process(
        self,
        username: str,
        port: int,
        process: multiprocessing.Process) -> None:
        """ Add a process and its metadata to the registry."""

        self.running_processes.append(
            (process, port, self.get_time(), username)
        )
        self.port_manager.update_available_ports()

    def terminate_target_processes(
        self,
        username: Optional[str] = None,
        session_name: Optional[str] = None,
        port: Optional[int] = None,
        timeout: Optional[int] = None):
        """ Terminate processes fulfilling one or more of these conditions:
                - registered username matches `username`
                - registered session_name matches `session_name`
                - registered port matches `port`
                - older than `timeout` (in seconds)

            Leave the arguments as `None` if they are to be ignored.
        """

        earliest = datetime.datetime(year=1, month=1, day=1)
        if timeout:
            earliest = self.get_time() - datetime.timedelta(seconds=timeout)

        for process, p_port, start_time, p_username in self.running_processes:
            cond_username = (p_username == username)
            cond_port = (p_port == port)
            cond_session = (process.name == session_name)
            cond_timeout = (start_time < earliest)

            if any((cond_username, cond_port, cond_session, cond_timeout)):
                process.terminate()
                time.sleep(1)
                # print(f'Terminating process {process.name} on port {p_port}')
                self.logger.info(
                    f'Terminating process {process.name} on port {p_port}')

    def cleanup_running_processes(self, **kwargs) -> int:
        """ Terminate *any* process fulfilling one or more of these conditions:
                - registered username matches `username`
                - registered session_name matches `session_name`
                - registered port matches `port`
                - older than `timeout` (in seconds)

            and *cleanup* the registry afterward.
        """

        before = len(self.running_processes)
        self.terminate_target_processes(**kwargs)

        self.port_manager.update_available_ports()
        self.running_processes = [
            (process, p, s, u)
            for (process, p, s, u) in self.running_processes
            if process.is_alive()
        ]
        after = len(self.running_processes)
        return before - after

    def already_has_instance(self, username: str) -> bool:
        if username in [x for (_, _, _, x) in self.running_processes]:
            self.logger.error(
                f"{username} already started a Prodigy instance")
            return True
        return False

    @classmethod
    def get_time(cls) -> datetime.datetime:
        return datetime.datetime.now()

class Progeny(object):
    """Spin, monitor and destroy Prodigy instances."""

    logger = logging.getLogger(__name__)

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
    def build_session_name(cls, username: str, uniquify: bool = True) -> str:
        """ Create a random session name to make the instance url unpredictable
            even if we know the user name. This is because the Prodigy instance
            itself is not password-protected, and so an attacker with the url
            would be able to access it.
        """
        if uniquify:
            return f'{username}-{hash(random.random())}'
        return username


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
            username: str,
            command: Optional[str] = None,
            prebaked: Optional[str] = None,
            recipe: Optional[str] = None,
            recipe_args: Optional[Union[Tuple[str], List[str]]] = None,
            recipe_kwargs: Optional[Dict[Any, Any]] = None,
            config: Optional[Dict[Any, Any]] = None,
            uniquify: bool = False) -> Tuple[str, int]:
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

        if self.registry.already_has_instance(username):
            raise ValueError(f"{username} already has a process running")
        port = self.port_manager.allocate_port()
        session_name = self.build_session_name(username, uniquify=uniquify)

        config = {**config, 'port': port}

        process = self.get_process(command, config, session_name)
        self.logger.info(f'Process created: ({username}, {session_name}, {port}) ')
        process.start()

        self.registry.register_process(username, port, process)

        return (port, session_name)
        # return f'127.0.0.1:{port}/?session={session_name}'
