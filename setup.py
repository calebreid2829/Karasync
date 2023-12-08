from setuptools import setup, find_packages

install_requirements = []

test_requirements = []

dependency_links = []

if __name__ == '__main__':
    setup(
        name='rekkasync',
        version='0.1.1',
        author="Caleb Reid",
        url="https://github.com/calebreid2829/Rekkasync",
        description="Module for simple use of multi-proccessing and multi-threading",
        packages=find_packages(),
        tests_require=test_requirements,
        install_requires=install_requirements,
        include_package_data=True,
    )

