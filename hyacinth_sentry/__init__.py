"""Shared package metadata and project-level paths."""

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = PACKAGE_DIR.parent

__all__ = ["PACKAGE_DIR", "PROJECT_DIR"]
