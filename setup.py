from setuptools import setup, find_packages

setup(
    name="airflow-enterprise",
    version="1.0.0",
    description="Enterprise-grade cross-DAG dependency orchestration for Apache Airflow",
    author="Platform Engineering Team",
    author_email="platform@company.com",
    url="https://github.com/niteshme21/Airflow",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "apache-airflow>=2.7.0",
        "sqlalchemy>=2.0.0",
        "prometheus-client>=0.19.0",
        "psycopg2-binary>=2.9.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
