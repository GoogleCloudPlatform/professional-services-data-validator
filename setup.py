import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = ["pandas", "google-api-python-client==1.7.11",
                    "google-cloud-bigquery==1.22.0", "ibis-framework==1.2.0"]

setuptools.setup(
    name="Data Validation Tool", # Replace with your own username
    version="0.0.1",
    author="Dylan Hercher",
    author_email="dhercher@google.com",
    description="A package to enable easy data validation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    packages=["data_validation", "data_validation.data_sources", "data_validation.query_builder"],
    # packages=setuptools.find_packages(include=["data_validation*"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=install_requires,
)
