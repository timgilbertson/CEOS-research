from setuptools import setup, find_packages

setup(
    name='ceos_indices',
    version='0.0.1',
    entry_points={
        "console_scripts": [
            "ceos_indices = ceos_indices.generate_indices:main"
        ]
    },
    packages=find_packages(),
    install_requires=[
    ],
    zip_safe=False
)