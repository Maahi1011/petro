# import setuptools
#
# with open("README.md", "r") as fh:
#     long_description = fh.read()
#
# setuptools.setup(
#     name="testpetro",
#     version="0.0.1",
#     author="maahi2710",
#     author_email="maheshwari.sathelli@gmail.com",
#     description="Package to test",
#     long_description=long_description,
#     long_description_content_type="text/markdown",
#     packages=setuptools.find_packages(),
#     classifiers=[
#         "Programming Language :: Python :: 3",
#         "License :: OSI Approved :: MIT License",
#         "Operating System :: OS Independent",
#     ],
#     python_requires='>=3.7',
# )
#
# from setuptools import setup
# setup(
#     name='mstest',
#     version='1.0',
#     packages=[ "mstest.common", "mstest.config", "mstest.usermanaged", "mstest"],
#     package_dir={
#         "mstest.common": "./mstest/common",
#         "mstest.config": "./mstest/config",
#         "mstest.usermanaged": "./mstest/usermanaged",
#         "mstest": "./mstest"
#
#     }
# )

from setuptools import setup
setup(
    name='petrodb',
    version='1.0',
    packages=[ "petrodb.common", "petrodb.config", "petrodb.usermanaged", "petrodb"],
    package_dir={
        "petrodb.common": "./petrodb/common",
        "petrodb.config": "./petrodb/config",
        "petrodb.usermanaged": "./petrodb/usermanaged",
        "petrodb": "./petrodb"

    }
)