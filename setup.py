import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="jsonstream",
    version="0.0.1",
    author="Jack Hargreaves",
    description="Load multiple delimited JSON documents from a single string or file-like object.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Dunes/json_stream",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    license='MIT',
    keywords='JSON stream load',
    py_modules=['jsonstream'],
)
