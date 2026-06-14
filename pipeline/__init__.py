"""实验数据处理流水线包"""
import logging
import sys

__version__ = "1.0.0"

_pipeline_logger = logging.getLogger("pipeline")
if not _pipeline_logger.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _handler.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
    _pipeline_logger.addHandler(_handler)
    _pipeline_logger.setLevel(logging.INFO)
    _pipeline_logger.propagate = False
