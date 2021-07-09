import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="solardb",
    version="0.0.1",
    author="Tomas Polasek",
    author_email="ipolasek@fit.vutbr.cz",
    description="Python API for the SolarDB photovoltaic dataset",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://cphoto.fit.vutbr.cz/solar",
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering",
    ],
    packages=setuptools.find_packages("./"),
    package_dir={ "solardb" : "solardb" },
    python_requires=">=3.7.1",
)
