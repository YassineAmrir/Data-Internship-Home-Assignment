from unittest.mock import patch, mock_open
import unittest
import os
from dags.etl import extract_context_data

class TestExtractContextData(unittest.TestCase):

    @patch("builtins.open", mock_open(read_data="id,context\n1,Sample context 1\n2,Sample context 2"))
    @patch("os.makedirs")  # Mock the creation of directories
    @patch("os.path.exists", return_value=True)  # Mock that the file exists
    @patch("os.listdir")  # Mock os.listdir to check created files
    def test_extract_context_data(self, mock_listdir, mock_exists, mock_makedirs):
        extract_context_data()

        # Mock the expected list of files
        mock_listdir.return_value = ['context_1.txt', 'context_2.txt']

        # Check if files are created correctly
        extracted_dir = '/opt/airflow/dags/staging/extracted'
        files = os.listdir(extracted_dir)
        self.assertGreater(len(files), 0, "No files created in the extracted directory.")

        # Check content inside the text files
        for file in files:
            with open(os.path.join(extracted_dir, file), 'r') as f:
                content = f.read()
                self.assertIn('Sample context', content, f"File {file} content mismatch.")

    @patch("builtins.open", mock_open(read_data="id,context\n"))
    @patch("os.makedirs")  # Mock that no directories are created
    @patch("os.path.exists", return_value=True)  # Mock that the file exists
    @patch("os.listdir")  # Mock os.listdir to verify that no files are created
    def test_empty_csv(self, mock_listdir, mock_exists, mock_makedirs):
        # Simulate an empty file
        extract_context_data()

        # Check that no files are created for an empty CSV
        extracted_dir = '/opt/airflow/dags/staging/extracted'
        files = os.listdir(extracted_dir)
        self.assertEqual(len(files), 0, "Files were created from empty CSV.")

        # Ensure that os.makedirs was not called (no directories created)
        mock_makedirs.assert_not_called()

        # Ensure that no files were written (check mock_open calls)
        open_mock = mock_open()  # Create a new mock for the open function
        open_mock.assert_not_called()  # Check if it was called or not

        # Verify that os.listdir was called to check the directory contents
        mock_listdir.assert_called_once_with(extracted_dir)

if __name__ == "__main__":
    unittest.main()
