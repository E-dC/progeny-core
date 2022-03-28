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

try:
    from .setup_logging import logging
except ImportError:
    from setup_logging import logging

class ProdigyController(object):
    """Spin, monitor and destroy Prodigy instances."""

    logger = logging.getLogger(__name__)

    def __init__(
        self,
        recipe_dir: Optional[str] = None,
        port_range: Union[range, Tuple[int, int], List[int]] = (8080, 8090)):
        """ Load recipes, setup a registry of running processes, setup a pool
            of usable ports, and start a timed cleanup process.
        """

        self.port_range = self.validate_port_range(port_range)
        self.update_available_ports()

        self.available_recipes = self.import_recipes(recipe_dir)

        self.running_processes = []

        signal.signal(signal.SIGTERM, self.catch_signal)
        signal.signal(signal.SIGINT, self.catch_signal)
        signal.signal(signal.SIGQUIT, self.catch_signal)

        self.scheduled_stopper = threading.Event()
        self.scheduled_cleaning(10, 300)

    def __del__(self):
        """ Make sure to stop the cleaning process when destroying instance"""
        self.scheduled_stopper.set()
        self.current_timer.cancel()

    def catch_signal(
        self, signum: int, frame: typing.types.FrameType):
        """ When catching SIGTERM, SIGINT or SIGQUIT, stop any running
            cleaning process before exiting
        """
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

    def update_available_ports(self) -> None:
        self.available_ports = set([
            p for p in self.port_range
            if not self.is_port_in_use(p)
        ])

    def register_process(
        self,
        username: str,
        port: int,
        process: multiprocessing.Process) -> None:
        """ Add a process and its metadata to the registry."""

        self.running_processes.append(
            (process, port, self.get_time(), username)
        )
        self.update_available_ports()

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

        self.update_available_ports()
        self.running_processes = [
            (process, p, s, u)
            for (process, p, s, u) in self.running_processes
            if process.is_alive()
        ]
        after = len(self.running_processes)
        return before - after

    @classmethod
    def import_recipes(cls, recipe_dir: Optional[str]) -> set:
        """ Make our custom recipes visible to Prodigy, so that we can call
            them by recipe name, and without having to give the file name as
            an argument when spinning an instance.

            Example: we'll be able to call
                prodigy my.recipe arg1 arg2
            instead of
                prodigy my.recipe arg1 arg2 -F filename_containing_my_recipe.py
        """
        def try_import(root: str, filename: str) -> set:
            o = set()
            fp = os.path.join(root, filename)
            if (fp.endswith('.py')
                and not filename.startswith('_')
                and not filename.startswith('loader')):

                cls.logger.info(
                    f"Loading recipes from {fp}")
                try:
                    o = set(prodigy.core.list_recipes(path=fp))
                except:
                    raise
            return o

        starting_recipes = set(prodigy.core.list_recipes())
        available_recipes = set(starting_recipes)

        if recipe_dir:
            cls.logger.info(
                f"Looking for recipes in {os.path.abspath(recipe_dir)}")
            for root, dirs, files in os.walk(recipe_dir):
                for filename in files:
                    available_recipes.update(try_import(root, filename))

        found = available_recipes - starting_recipes
        suffix = f': {sorted(found)}' if found else ''
        cls.logger.info(
            f"{len(found)} additional recipes found{suffix}")

        return available_recipes


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

    @classmethod
    def get_time(cls) -> datetime.datetime:
        return datetime.datetime.now()

    @classmethod
    def build_session_name(cls, username: str) -> str:
        """ Create a random session name to make the instance url unpredictable
            even if we know the user name. This is because the Prodigy instance
            itself is not password-protected, and so an attacker with the url
            would be able to access it.
        """
        return f'{username}-{hash(random.random())}'

    def construct_loader_command(
        self, loader: Optional[str], *args) -> Optional[str]:
        """ Construct a *default loader* command which will become part of
            the full Prodigy command (loader instructions + recipe).
        """

        default_loaders = {
            'jsonl', 'json', 'csv', 'txt',
            'images', 'images-server',
            'audio', 'audio-server',
            'video', 'video-server',
        }

        command = None
        if loader in default_loaders and args:
            command = f'{args[0]} --loader {loader}'

        return command

    def construct_recipe_command(
        self, recipe: str, *args, **kwargs) -> str:
        """ Construct a recipe command, which will become part of the full
            Prodigy command (loader instructions + recipe).
        """
        assert recipe in self.available_recipes

        command = f"{recipe}"
        if args:
            command = f"{command} {' '.join(args)}"
        if kwargs:
            l = []
            [l.extend((k, v)) for (k, v) in kwargs.items()]
            command = f"{command} {' '.join(l)}"

        return command

    def construct_prodigy_command(
        self, recipe_command: str, loader_command: Optional[str]) -> str:

        """ Construct the full Prodigy command, concatenating the recipe and the
            loader instructions.
        """

        if loader_command:
            return f'{recipe_command} {loader_command}'

        return recipe_command

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


    def already_has_instance(self, username: str) -> bool:
        if username in [x for (_, _, _, x) in self.running_processes]:
            self.logger.error(
                f"{username} already started a Prodigy instance")
            return True
        return False

    def allocate_port(self) -> int:
        for port in self.port_range:
            if self.is_port_in_use(port):
                continue
            self.available_ports.remove(port)
            return port
        else:
            self.logger.error("No ports available")
            raise RuntimeError("No ports available")

    def get_process(
        self,
        command: str,
        config: Dict[str, Any],
        session_name: str) -> multiprocessing.Process:
        """ Prepare a new process on which we can start a Prodigy instance"""

        env = os.environ.copy()
        p = multiprocessing.Process(
                target=self.prodigy_serve,
                name=f'progeny-{session_name}',
                args=(command, config, env, session_name))

        return p


    def spin(
        self,
        username: str,
        recipe: str,
        recipe_args: Tuple[str],
        recipe_kwargs: Optional[Dict[Any,Any]] = None,
        loader: Optional[str] = None,
        loader_args: Optional[Tuple[str]] = None) -> Tuple[int, str]:
        """ Check we can start a new instance, construct the instance arguments
            and configuration, prepare the process, start it, and update the
            registry appropriately.

            `recipe_args`'s first element should pretty much always be the
            dataset name (in which we store the annotations)

            `loader` and `loader_args` are useful when you want to use a default
            Prodigy recipe and load the data to annotate straight from a flat
            file (see `construct_loader_command`)
        """


        # username: str,
        # recipe: str = 'textcat.rasabot-single',
        # recipe_args: Tuple[str] = ('brs-bot', 'eng'),# 'configs/brs_bot_config.yml'),
        # recipe_kwargs: Optional[Dict[Any,Any]] = None,
        # loader: Optional[str] = None,
        # loader_args: Optional[Tuple[str]] = None) -> Tuple[int, str]:



        if self.already_has_instance(username):
            raise ValueError(f"{username} already has a process running")

        port = self.allocate_port()

        if not recipe_kwargs:
            recipe_kwargs = {}
        if not loader_args:
            loader_args = ()

        recipe_command = self.construct_recipe_command(
            recipe, *recipe_args, **recipe_kwargs)
        loader_command = self.construct_loader_command(
            loader, *loader_args)
        command = self.construct_prodigy_command(
            recipe_command, loader_command)

        config = {'port': port, 'feed_overlap': False}
        # session_name = self.build_session_name(username)
        session_name = username

        process = self.get_process(command, config, session_name)
        print('process created')
        process.start()

        self.register_process(username, port, process)

        return (port, session_name)
        # return f'127.0.0.1:{port}/?session={session_name}'
