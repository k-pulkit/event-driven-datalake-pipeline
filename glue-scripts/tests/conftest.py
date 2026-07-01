# or ABOUTME: Pytest configuration file dynamically mocking proprietary AWS Glue modules for local unit testing

import sys
from unittest.mock import MagicMock

# Register pure MagicMock objects in sys.modules to satisfy top-level import statements during testing
sys.modules["awsglue"] = MagicMock()
sys.modules["awsglue.utils"] = MagicMock()
sys.modules["awsglue.context"] = MagicMock()
sys.modules["awsglue.job"] = MagicMock()
