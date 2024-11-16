#!/bin/bash

# Set the Python virtual environment path if needed
# VENV_PATH="/path/to/your/venv"

# Activate the virtual environment if needed
# source "$VENV_PATH/bin/activate"

# Check if pytest is installed
if ! command -v pytest &> /dev/null
then
    echo "pytest is not installed. Installing..."
    pip install pytest
fi

# Run the tests
echo "Running tests..."
pytest tests/  # Adjust the path to your tests directory if necessary

# Capture the exit status of pytest
TEST_STATUS=$?

# Optional: Deactivate the virtual environment if it was activated
# deactivate

# Check if tests passed or failed
if [ $TEST_STATUS -eq 0 ]; then
    echo "All tests passed successfully."
else
    echo "Some tests failed. Please check the output above."
fi

exit $TEST_STATUS