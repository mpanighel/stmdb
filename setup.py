import setuptools

from stmdb._version import __version__

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="stmdb",
    version=__version__,
    author="Mirco Panighel",
    author_email="panighel@iom.cnr.it",
    description="Python code to manage STM images database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mpanighel/stmdb",
    license="MIT",
    packages=setuptools.find_packages(exclude=["test_files"]),
    install_requires=[
        "paramiko",
        "scp"
    ],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
