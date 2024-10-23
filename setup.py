from setuptools import setup, find_packages


from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

setup(
    name='python-esios',
    version='0.2.4',
    packages=find_packages(),
    install_requires=[
        'requests',
        'pandas',
    ],
    author="Jesús López",
    author_email="jesus.lopez@datons.ai",
    description="A Python wrapper for the ESIOS API",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="https://github.com/datons/python-esios",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
