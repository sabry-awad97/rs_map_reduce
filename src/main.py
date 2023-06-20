import random
from threading import Thread
import os


class GenericInputData:
    """
    This class represents a generic input data object that can be used as a base class for other input data classes. It defines two abstract methods: `read` and `generate_inputs`, which must be implemented by any concrete subclasses.

    Attributes:
        None

    Methods:
        read(): Reads the input data and returns it in a format that can be processed by the worker classes.
        generate_inputs(config): Generates a list of input data objects based on a configuration object.
    """

    def read(self):
        """
        Reads the input data and returns it in a format that can be processed by the worker classes.

        Raises:
            NotImplementedError: This method must be implemented by any concrete subclass.
        """
        raise NotImplementedError

    @classmethod
    def generate_inputs(cls, config):
        """
        Generates a list of input data objects based on a configuration object.

        Args:
            config (object): A configuration object that specifies how to generate the input data objects.

        Raises:
            NotImplementedError: This method must be implemented by any concrete subclass.
        """
        raise NotImplementedError


class PathInputData(GenericInputData):
    """
    This class represents input data that is stored in a file on the local file system. It inherits from the GenericInputData class and overrides the read method to read the input data from the file. It also defines a class method called generate_inputs that can be used to generate a list of PathInputData objects based on a configuration object.

    Attributes:
        path (str): The path to the input data file.

    Methods:
        read(): Reads the input data from the file and returns it as a string.
        generate_inputs(config): Generates a list of PathInputData objects based on a configuration object.
    """

    def __init__(self, path):
        """
        Initializes a new PathInputData object with the given file path.

        Args:
            path (str): The path to the input data file.
        """
        self.path = path

    def read(self):
        """
        Reads the input data from the file and returns it as a string.

        Returns:
            str: The input data from the file.
        """
        with open(self.path) as f:
            return f.read()

    @classmethod
    def generate_inputs(cls, config):
        """
        Generates a list of PathInputData objects based on a configuration object.

        Args:
            config (dict): A dictionary containing the 'data_dir' key with the path to the directory containing the input data files.

        Yields:
            PathInputData: A PathInputData object for each input data file in the directory.
        """
        data_dir = config['data_dir']
        for name in os.listdir(data_dir):
            yield cls(os.path.join(data_dir, name))


class GenericWorker:
    """
    This class represents a generic worker object that can be used as a base class for other worker classes. It defines three abstract methods: `map`, `reduce`, and `create_workers`, which must be implemented by any concrete subclasses.

    Attributes:
        input_data (GenericInputData): The input data for the worker.
        result (object): The result of the worker's computation.

    Methods:
        map(): Maps the input data to a result using the map function.
        reduce(other): Reduces two workers to a single worker using the reduce function.
        create_workers(input_class, config): Creates a list of workers based on a configuration object and an input data class.
    """

    def __init__(self, input_data):
        """
        Initializes a new GenericWorker object with the given input data.

        Args:
            input_data (GenericInputData): The input data for the worker.
        """
        self.input_data = input_data
        self.result = None

    def map(self):
        """
        Maps the input data to a result using the map function.

        Raises:
            NotImplementedError: This method must be implemented by any concrete subclass.
        """
        raise NotImplementedError

    def reduce(self, other):
        """
        Reduces two workers to a single worker using the reduce function.

        Args:
            other (GenericWorker): The other worker to reduce with.

        Raises:
            NotImplementedError: This method must be implemented by any concrete subclass.
        """
        raise NotImplementedError

    @classmethod
    def create_workers(cls, input_class, config):
        """
        Creates a list of workers based on a configuration object and an input data class.

        Args:
            input_class (type): The input data class to use for creating the workers.
            config (object): A configuration object that specifies how to create the workers.

        Returns:
            list: A list of workers created based on the input data class and configuration object.
        """
        workers = []
        for input_data in input_class.generate_inputs(config):
            workers.append(cls(input_data))
        return workers


class LineCountWorker(GenericWorker):
    """
    This class represents a worker that counts the number of lines in input data. It inherits from the Worker class and overrides the map and reduce methods to perform the count operation.

    Attributes:
        input_data (InputData): The input data for the worker.
        result (int): The number of lines in the input data.

    Methods:
        map(): Counts the number of lines in the input data and returns the result.
        reduce(other): Adds the number of lines in another worker to the current worker's result and returns the result.
    """

    def map(self):
        """
        Counts the number of lines in the input data and returns the result.

        Returns:
            int: The number of lines in the input data.
        """
        data = self.input_data.read()
        self.result = data.count('\n')

    def reduce(self, other):
        """
        Adds the number of lines in another worker to the current worker's result and returns the result.

        Args:
            other (LineCountWorker): The other worker to add the number of lines to.

        Returns:
            int: The total number of lines in the input data.
        """
        self.result += other.result
        return self.result


def execute(workers):
    """
    Executes a MapReduce operation on a list of workers.

    Args:
        workers (list): A list of Worker objects to execute the MapReduce operation on.

    Returns:
        object: The result of the MapReduce operation.
    """
    threads = [Thread(target=w.map) for w in workers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    first, *rest = workers
    for worker in rest:
        first.reduce(worker)
    return first.result


def mapreduce(worker_class, input_class, config):
    workers = worker_class.create_workers(input_class, config)
    return execute(workers)
