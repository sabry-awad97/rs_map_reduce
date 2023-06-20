import unittest
import tempfile
import shutil
from threading import Thread
import os
from main import PathInputData, LineCountWorker, generate_inputs, create_workers, execute, mapreduce


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

    def test_generate_inputs(self):
        # Generate inputs from the temporary directory
        inputs = list(generate_inputs(self.temp_dir))

        # Check the number of input data objects
        self.assertEqual(len(inputs), 3)

        # Check the paths of the input data objects
        expected_paths = [self.file1, self.file2, self.file3]
        actual_paths = [input_data.path for input_data in inputs]
        self.assertCountEqual(actual_paths, expected_paths)

    def test_create_workers(self):
        # Create a list of input data objects
        inputs = list(generate_inputs(self.temp_dir))

        # Create workers from the input data objects
        workers = create_workers(inputs)

        # Check the number of workers
        self.assertEqual(len(workers), 3)

        # Check the input data paths of the workers
        expected_paths = [self.file1, self.file2, self.file3]
        actual_paths = [worker.input_data.path for worker in workers]
        self.assertCountEqual(actual_paths, expected_paths)

    def test_execute(self):
        # Create a list of workers
        inputs = list(generate_inputs(self.temp_dir))
        workers = create_workers(inputs)

        # Execute the MapReduce operation
        result = execute(workers)

        # Check the result
        self.assertEqual(result, 9)

    def test_mapreduce(self):
        # Execute the MapReduce operation
        result = mapreduce(self.temp_dir)

        # Check the result
        self.assertEqual(result, 9)


if __name__ == '__main__':
    unittest.main()
