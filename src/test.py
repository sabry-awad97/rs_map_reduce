import unittest
import tempfile
import shutil
import os
from main import PathInputData, LineCountWorker, execute, mapreduce


class MapReduceTestCase(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory for test data
        self.temp_dir = tempfile.mkdtemp()

        # Create test input data files
        self.file1 = os.path.join(self.temp_dir, 'file1.txt')
        self.file2 = os.path.join(self.temp_dir, 'file2.txt')
        self.file3 = os.path.join(self.temp_dir, 'file3.txt')

        with open(self.file1, 'w') as f:
            f.write('Line 1\nLine 2\nLine 3\n')

        with open(self.file2, 'w') as f:
            f.write('Line 4\nLine 5\n')

        with open(self.file3, 'w') as f:
            f.write('Line 6\nLine 7\nLine 8\nLine 9\n')

    def tearDown(self):
        # Remove the temporary directory and files
        shutil.rmtree(self.temp_dir)

    def test_line_count_worker(self):
        # Create a PathInputData instance for the first file
        input_data = PathInputData(self.file1)

        # Create a LineCountWorker instance with the input data
        worker = LineCountWorker(input_data)

        # Call the map() method
        worker.map()

        # Check the result
        self.assertEqual(worker.result, 3)

    def test_execute(self):
        # Create a list of workers
        inputs = [PathInputData(self.file1), PathInputData(
            self.file2), PathInputData(self.file3)]
        workers = [LineCountWorker(input_data) for input_data in inputs]

        # Execute the MapReduce operation
        result = execute(workers)

        # Check the result
        self.assertEqual(result, 9)

    def test_mapreduce(self):
        # Create a configuration object
        config = {'data_dir': self.temp_dir}

        # Execute the MapReduce operation
        result = mapreduce(LineCountWorker, PathInputData, config)

        # Check the result
        self.assertEqual(result, 9)


if __name__ == '__main__':
    unittest.main()
