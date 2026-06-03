"""Модуль работы с очередью задач."""

from .manager import (
    convert_job_to_config,
    get_next_job_from_queue,
    mark_job_done,
    mark_job_error,
)

__all__ = [
    'get_next_job_from_queue',
    'mark_job_done',
    'mark_job_error',
    'convert_job_to_config',
]