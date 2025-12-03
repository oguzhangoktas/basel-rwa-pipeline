"""
Basel RWA Pipeline Package Setup

Author: Portfolio Project
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="basel-rwa-pipeline",
    version="1.0.0",
    author="Oğuzhan Göktaş",
    author_email="oguzhangoktas22@gmail.com",
    description="Production-grade Basel III RWA calculation pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/oguzhangoktas/basel-rwa-pipeline",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.3.1",
            "pytest-cov>=4.1.0",
            "black>=23.3.0",
            "pylint>=2.17.4",
            "mypy>=1.3.0",
        ],
        "docs": [
            "sphinx>=6.2.1",
            "sphinx-rtd-theme>=1.2.1",
        ],
    },
    entry_points={
        "console_scripts": [
            "rwa-calculate=glue_jobs.stage2_calculation:main",
            "rwa-generate-sample=sample_data.generate_sample_data:main",
        ],
    },
)