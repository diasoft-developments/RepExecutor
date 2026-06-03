"""Модуль API."""

from .app import app, api_metrics
from .routes import router

__all__ = ['app', 'api_metrics', 'router']