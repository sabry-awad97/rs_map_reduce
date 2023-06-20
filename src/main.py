class InputData:
    """
    This class represents the input data for a program. It is an abstract class that defines the interface for reading input data.

    Attributes:
        path (str): The path to the input data file.

    Methods:
        read(): Reads the input data from the file specified by the path attribute and returns it as a string.
    """

    def __init__(self, path):
        """
        Initializes the InputData object with the given path.

        Args:
            path (str): The path to the input data file.
        """
        self.path = path

    def read(self):
        """
        Reads the input data from the file specified by the path attribute and returns it as a string.

        Returns:
            str: The input data read from the file.
        """
        raise NotImplementedError


class PathInputData(InputData):
    """
    This class represents the input data for a program that is stored in a file. It inherits from the InputData class and overrides the read method to read data from a file.

    Attributes:
        path (str): The path to the input data file.

    Methods:
        read(): Reads the input data from the file specified by the path attribute and returns it as a string.
    """

    def read(self):
        """
        Reads the input data from the file specified by the path attribute and returns it as a string.

        Returns:
            str: The input data read from the file.
        """
        with open(self.path) as f:
            return f.read()


class Worker:
    """
    This class represents a worker in a MapReduce framework. It is an abstract class that defines the interface for map and reduce operations.

    Attributes:
        input_data (InputData): The input data for the worker.
        result (object): The result of the map or reduce operation.

    Methods:
        map(): Performs the map operation on the input data and returns the result.
        reduce(other): Performs the reduce operation on the current worker and another worker, and returns the result.
    """

    def __init__(self, input_data):
        """
        Initializes the Worker object with the given input data.

        Args:
            input_data (InputData): The input data for the worker.
        """
        self.input_data = input_data
        self.result = None

    def map(self):
        """
        Performs the map operation on the input data and returns the result.

        Returns:
            object: The result of the map operation.
        """
        raise NotImplementedError

    def reduce(self, other):
        """
        Performs the reduce operation on the current worker and another worker, and returns the result.

        Args:
            other (Worker): The other worker to perform the reduce operation with.

        Returns:
            object: The result of the reduce operation.
        """
        raise NotImplementedError


class LineCountWorker(Worker):
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
