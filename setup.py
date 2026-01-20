#!/usr/bin/env python
"""
E-Commerce Analytics Platform Setup
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="ecommerce-analytics",
    version="1.0.0",
    author="Ali Abbass",
    author_email="ali.abbass@example.com",
    description="Production-grade real-time e-commerce analytics platform",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/OTE22/E-COMMERCE-PIPELINE",
    project_urls={
        "Bug Tracker": "https://github.com/OTE22/E-COMMERCE-PIPELINE/issues",
        "Documentation": "https://github.com/OTE22/E-COMMERCE-PIPELINE#readme",
        "Source Code": "https://github.com/OTE22/E-COMMERCE-PIPELINE",
    },
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Framework :: FastAPI",
        "Topic :: Internet :: WWW/HTTP :: HTTP Servers",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "pytest-asyncio>=0.21.0",
            "black>=23.0.0",
            "ruff>=0.1.0",
            "mypy>=1.5.0",
            "pre-commit>=3.5.0",
        ],
        "ml": [
            "scikit-learn>=1.3.0",
            "xgboost>=2.0.0",
            "lightgbm>=4.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "ecommerce-api=src.main:app",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords=[
        "ecommerce",
        "analytics",
        "fastapi",
        "data-pipeline",
        "etl",
        "machine-learning",
        "postgresql",
        "redis",
        "kafka",
    ],
)
